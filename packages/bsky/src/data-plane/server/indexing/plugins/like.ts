import { Selectable } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as Like from '../../../../lexicon/types/app/bsky/feed/like'
import * as lex from '../../../../lexicon/lexicons'
import RecordProcessor from '../processor'
import { countAll, excluded, valuesAliased } from '../../db/util'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { RecordProcessor } from '../processor'

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
  return db
    .insertInto('like')
    .values(
      records.map(({ uri, cid, obj, timestamp }) => ({
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        subject: obj.subject.uri,
        subjectCid: obj.subject.cid,
        createdAt: normalizeDatetimeAlways(obj.createdAt),
        indexedAt: timestamp,
      })),
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
  const origDb = db
  const likeCountQbs = db
    .with('input_values', (db) =>
      db
        .selectFrom(
          valuesAliased(
            likes.map((l) => ({ uri: l.subject })),
            't',
          ),
        )
        .select('uri'),
    )
    .with('insert_counts', (db) =>
      db
        .insertInto('post_agg')
        .columns(['uri', 'likeCount'])
        .expression(
          db
            .selectFrom('input_values as v')
            .leftJoin('like', (join) => join.on('like.subject', '=', 'v.uri'))
            .select(['v.uri', (eb) => eb.fn.count('like.uri').as('likeCount')])
            .groupBy('v.uri'),
        )
        .onConflict((oc) =>
          oc
            .column('uri')
            .doUpdateSet({ likeCount: excluded(origDb, 'likeCount') }),
        ),
    )
    .selectFrom('insert_counts')
  await likeCountQbs.execute()
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
