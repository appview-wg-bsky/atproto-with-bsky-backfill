import { Selectable, sql } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import * as lex from '../../../../lexicon/lexicons'
import * as Repost from '../../../../lexicon/types/app/bsky/feed/repost'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { countAll, excluded } from '../../db/util'
import { copyIntoTable, executeRaw, transpose } from '../../util'
import RecordProcessor from '../processor'

const lexId = lex.ids.AppBskyFeedRepost
type IndexedRepost = Selectable<DatabaseSchemaType['repost']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Repost.Record,
  timestamp: string,
): Promise<IndexedRepost | null> => {
  const repost = {
    uri: uri.toString(),
    cid: cid.toString(),
    creator: uri.host,
    subject: obj.subject.uri,
    subjectCid: obj.subject.cid,
    createdAt: normalizeDatetimeAlways(obj.createdAt),
    indexedAt: timestamp,
  }
  const [inserted] = await Promise.all([
    db
      .insertInto('repost')
      .values(repost)
      .onConflict((oc) => oc.doNothing())
      .returningAll()
      .executeTakeFirst(),
    db
      .insertInto('feed_item')
      .values({
        type: 'repost',
        uri: repost.uri,
        cid: repost.cid,
        postUri: repost.subject,
        originatorDid: repost.creator,
        sortAt:
          repost.indexedAt < repost.createdAt
            ? repost.indexedAt
            : repost.createdAt,
      })
      .onConflict((oc) => oc.doNothing())
      .executeTakeFirst(),
  ])

  return inserted || null
}

// const insertBulkFn = async (
//   db: DatabaseSchema,
//   records: {
//     uri: AtUri
//     cid: CID
//     obj: Repost.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedRepost>> => {
//   const toInsertRepost = transpose(records, ({ uri, cid, obj, timestamp }) => [
//     /* uri: */ uri.toString(),
//     /* cid: */ cid.toString(),
//     /* creator: */ uri.host,
//     /* subject: */ obj.subject.uri,
//     /* subjectCid: */ obj.subject.cid,
//     /* createdAt: */ normalizeDatetimeAlways(obj.createdAt),
//     /* indexedAt: */ timestamp,
//   ])
//
//   const toInsertFeedItem = transpose(
//     records,
//     ({ uri, cid, obj, timestamp }) => [
//       /* type: */ 'post',
//       /* uri: */ uri.toString(),
//       /* cid: */ cid.toString(),
//       /* postUri: */ obj.subject.uri,
//       /* originatorDid: */ uri.host,
//       /* sortAt: */ timestamp < normalizeDatetimeAlways(obj.createdAt)
//         ? timestamp
//         : normalizeDatetimeAlways(obj.createdAt),
//     ],
//   )
//
//   const [{ rows: inserted }] = await Promise.all([
//     executeRaw<IndexedRepost>(
//       db,
//       `
//           INSERT INTO repost ("uri", "cid", "creator", "subject", "subjectCid", "createdAt", "indexedAt")
//           SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
//           ON CONFLICT DO NOTHING
//           RETURNING *
//         `,
//       toInsertRepost,
//     ).catch((e) => {
//       throw new Error('Failed to insert reposts', { cause: e })
//     }),
//     executeRaw(
//       db,
//       `
//           INSERT INTO feed_item ("type", "uri", "cid", "postUri", "originatorDid", "sortAt")
//           SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
//           ON CONFLICT DO NOTHING
//           RETURNING *
//         `,
//       toInsertFeedItem,
//     ).catch((e) => {
//       throw new Error('Failed to insert repost feed items', { cause: e })
//     }),
//   ])
//   return inserted || []
// }

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: Repost.Record
    timestamp: string
  }[],
): Promise<Array<IndexedRepost>> => {
  const inserted = await copyIntoTable(
    db.pool,
    'repost',
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
  await copyIntoTable(
    db.pool,
    'feed_item',
    ['type', 'uri', 'cid', 'postUri', 'originatorDid', 'sortAt'],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      const sortAt =
        new Date(createdAt).getTime() < new Date(indexedAt).getTime()
          ? createdAt
          : indexedAt
      return {
        type: 'post',
        uri: uri.toString(),
        cid: cid.toString(),
        postUri: obj.subject.uri,
        originatorDid: uri.host,
        sortAt,
      }
    }),
  )
  return inserted
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: Repost.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('repost')
    .where('creator', '=', uri.host)
    .where('subject', '=', obj.subject.uri)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = (obj: IndexedRepost) => {
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
          reason: 'repost' as const,
          reasonSubject: subjectUri.toString(),
          sortAt: obj.sortAt,
        },
      ]
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedRepost | null> => {
  const uriStr = uri.toString()
  const [deleted] = await Promise.all([
    db
      .deleteFrom('repost')
      .where('uri', '=', uriStr)
      .returningAll()
      .executeTakeFirst(),
    db.deleteFrom('feed_item').where('uri', '=', uriStr).executeTakeFirst(),
  ])
  return deleted || null
}

const notifsForDelete = (
  deleted: IndexedRepost,
  replacedBy: IndexedRepost | null,
) => {
  const toDelete = replacedBy ? [] : [deleted.uri]
  return { notifs: [], toDelete }
}

const updateAggregates = async (db: DatabaseSchema, repost: IndexedRepost) => {
  const repostCountQb = db
    .insertInto('post_agg')
    .values({
      uri: repost.subject,
      repostCount: db
        .selectFrom('repost')
        .where('repost.subject', '=', repost.subject)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc
        .column('uri')
        .doUpdateSet({ repostCount: excluded(db, 'repostCount') }),
    )
  await repostCountQb.execute()
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  reposts: IndexedRepost[],
) => {
  const repostCountQbs = sql`
    WITH input_values (uri) AS (
      SELECT * FROM UNNEST(${sql`${[reposts.map((r) => r.subject)]}::text[]`})
    )
    INSERT INTO post_agg ("uri", "repostCount")
    SELECT
      v.uri,
      count(repost.uri) AS repostCount
    FROM
      input_values AS v
      LEFT JOIN repost ON repost.subject = v.uri
    GROUP BY v.uri
    ON CONFLICT (uri) DO UPDATE SET "repostCount" = excluded."repostCount"
  `
  await repostCountQbs.execute(db).catch((e) => {
    throw new Error('Failed to update repost aggregates', { cause: e })
  })
}

export type PluginType = RecordProcessor<Repost.Record, IndexedRepost>

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
