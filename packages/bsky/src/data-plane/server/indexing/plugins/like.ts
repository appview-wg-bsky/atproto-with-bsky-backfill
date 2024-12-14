import { RawBuilder, Selectable, sql } from 'kysely'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import * as Like from '../../../../lexicon/types/app/bsky/feed/like'
import * as lex from '../../../../lexicon/lexicons'
import RecordProcessor from '../processor'
import { countAll, excluded } from '../../db/util'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { BackgroundQueue } from '../../background'

const lexId = lex.ids.AppBskyFeedLike
type IndexedLike = Selectable<DatabaseSchemaType['like']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Like.Record,
  timestamp: string,
): Promise<IndexedLike | null> => {
  const inserted = await db
    .insertInto('like')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      subject: obj.subject.uri,
      subjectCid: obj.subject.cid,
      createdAt: normalizeDatetimeAlways(obj.createdAt),
      indexedAt: timestamp,
    })
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .executeTakeFirst()
  return inserted || null
}

const insertBulkFn = async (
  db: DatabaseSchema,
  records: {
    uri: AtUri
    cid: CID
    obj: Like.Record
    timestamp: string
  }[],
): Promise<Array<IndexedLike>> => {
  // Transpose into an array per column to bypass the length limit on VALUES lists
  // sql.literal() is unsafe, so we compromise by not calling it on user-provided values
  const toInsert: [
    uri: Array<RawBuilder<unknown>>,
    cid: Array<RawBuilder<unknown>>,
    creator: Array<RawBuilder<unknown>>,
    subject: Array<string>,
    subjectCid: Array<string>,
    createdAt: Array<RawBuilder<unknown>>,
    indexedAt: Array<RawBuilder<unknown>>,
  ] = [[], [], [], [], [], [], []]
  for (const { uri, cid, obj, timestamp } of records) {
    toInsert[0].push(sql.literal(uri.toString()))
    toInsert[1].push(sql.literal(cid.toString()))
    toInsert[2].push(sql.literal(uri.host))
    toInsert[3].push(obj.subject.uri)
    toInsert[4].push(obj.subject.cid)
    toInsert[5].push(sql.literal(normalizeDatetimeAlways(obj.createdAt)))
    toInsert[6].push(sql.literal(timestamp))
  }
  return db
    .insertInto('like')
    .expression(
      db
        .selectFrom(
          sql<IndexedLike>`
            unnest(${sql.join(
              toInsert.map((arr) => sql`ARRAY[${sql.join(arr)}]`),
            )})
          `.as<'l'>(
            sql`l("uri", "cid", "creator", "subject", "subjectCid", "createdAt", "indexedAt")`,
          ),
        )
        .selectAll(),
    )
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .execute()
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: Like.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('like')
    .where('creator', '=', uri.host)
    .where('subject', '=', obj.subject.uri)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = (obj: IndexedLike) => {
  const subjectUri = new AtUri(obj.subject)
  // prevent self-notifications
  const isSelf = subjectUri.host === obj.creator
  return isSelf
    ? []
    : [
        {
          did: subjectUri.host,
          author: obj.creator,
          recordUri: obj.uri,
          recordCid: obj.cid,
          reason: 'like' as const,
          reasonSubject: subjectUri.toString(),
          sortAt: obj.sortAt,
        },
      ]
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedLike | null> => {
  const deleted = await db
    .deleteFrom('like')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = (
  deleted: IndexedLike,
  replacedBy: IndexedLike | null,
) => {
  const toDelete = replacedBy ? [] : [deleted.uri]
  return { notifs: [], toDelete }
}

const updateAggregates = async (db: DatabaseSchema, like: IndexedLike) => {
  const likeCountQb = db
    .insertInto('post_agg')
    .values({
      uri: like.subject,
      likeCount: db
        .selectFrom('like')
        .where('like.subject', '=', like.subject)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc.column('uri').doUpdateSet({ likeCount: excluded(db, 'likeCount') }),
    )
  await likeCountQb.execute()
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  likes: IndexedLike[],
) => {
  const likeCountQbs = sql`
    WITH input_values (uri) AS (
      SELECT * FROM unnest(${sql`${[likes.map((l) => l.subject)]}::text[]`})
    )
    INSERT INTO post_agg ("uri", "likeCount")
    SELECT
      v.uri,
      count(like.uri) AS likeCount
    FROM
      input_values AS v
      LEFT JOIN like ON like.subject = v.uri
    GROUP BY v.uri
    ON CONFLICT (uri) DO UPDATE SET "likeCount" = excluded."likeCount"
  `
  await likeCountQbs.execute(db)
}

export type PluginType = RecordProcessor<Like.Record, IndexedLike>

export const makePlugin = (
  db: Database,
  background: BackgroundQueue,
): PluginType => {
  return new RecordProcessor(db, background, {
    lexId,
    insertFn,
    insertBulkFn,
    findDuplicate,
    deleteFn,
    notifsForInsert,
    notifsForDelete,
    updateAggregates,
    updateAggregatesBulk,
  })
}

export default makePlugin
