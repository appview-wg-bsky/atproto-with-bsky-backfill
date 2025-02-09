import { Selectable, sql } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as Like from '../../../../lexicon/types/app/bsky/feed/like'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { countAll, excluded } from '../../db/util'
import { copyIntoTable } from '../../util'
import RecordProcessor from '../processor'

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

// const insertBulkFn = async (
//   db: DatabaseSchema,
//   records: {
//     uri: AtUri
//     cid: CID
//     obj: Like.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedLike>> => {
//   const toInsert = transpose(records, ({ uri, cid, obj, timestamp }) => [
//     /* uri: */ uri.toString(),
//     /* cid: */ cid.toString(),
//     /* creator: */ uri.host,
//     /* subject: */ obj.subject.uri,
//     /* subjectCid: */ obj.subject.cid,
//     /* createdAt: */ normalizeDatetimeAlways(obj.createdAt),
//     /* indexedAt: */ timestamp,
//   ])
//
//   const { rows } = await executeRaw<IndexedLike>(
//     db,
//     `
//           INSERT INTO "like" ("uri", "cid", "creator", "subject", "subjectCid", "createdAt", "indexedAt")
//             SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
//           ON CONFLICT DO NOTHING
//           RETURNING *
//         `,
//     toInsert,
//   ).catch((e) => {
//     throw new Error('Failed to insert likes', { cause: e })
//   })
//   return rows
// }

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: Like.Record
    timestamp: string
  }[],
): Promise<Array<IndexedLike>> => {
  return copyIntoTable(
    db.pool,
    'like',
    [
      'uri',
      'cid',
      'creator',
      'subject',
      'subjectCid',
      'createdAt',
      'indexedAt',
    ],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      const sortAt =
        new Date(createdAt).getTime() < new Date(indexedAt).getTime()
          ? createdAt
          : indexedAt
      return {
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        subject: obj.subject.uri,
        subjectCid: obj.subject.cid,
        createdAt,
        indexedAt,
        sortAt,
      }
    }),
  )
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
      SELECT * FROM unnest(${sql`${[likes.map((l) => l.subject)]}`}::text[])
    )
    INSERT INTO post_agg ("uri", "likeCount")
    SELECT
      "v"."uri",
      count("like"."uri") AS "likeCount"
    FROM
      "input_values" AS "v"
      LEFT JOIN "like" ON "like"."subject" = "v"."uri"
    GROUP BY "v"."uri"
    ON CONFLICT (uri) DO UPDATE SET "likeCount" = "excluded"."likeCount"
  `
  await likeCountQbs.execute(db).catch((e) => {
    throw new Error('Failed to update aggregates', { cause: e })
  })
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
