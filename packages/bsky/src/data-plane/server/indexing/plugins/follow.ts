import { Selectable, sql } from 'kysely'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import * as Follow from '../../../../lexicon/types/app/bsky/graph/follow'
import * as lex from '../../../../lexicon/lexicons'
import RecordProcessor from '../processor'
import { Database } from '../../db'
import { countAll, excluded } from '../../db/util'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { BackgroundQueue } from '../../background'
import { executeRaw, transpose } from '../../util'

const lexId = lex.ids.AppBskyGraphFollow
type IndexedFollow = Selectable<DatabaseSchemaType['follow']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Follow.Record,
  timestamp: string,
): Promise<IndexedFollow | null> => {
  const inserted = await db
    .insertInto('follow')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      subjectDid: obj.subject,
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
    obj: Follow.Record
    timestamp: string
  }[],
): Promise<Array<IndexedFollow>> => {
  const toInsert = transpose(records, ({ uri, cid, obj, timestamp }) => [
    uri.toString(),
    cid.toString(),
    uri.host,
    obj.subject,
    normalizeDatetimeAlways(obj.createdAt),
    timestamp,
  ])

  const { rows } = await executeRaw<IndexedFollow>(
    db,
    `
      INSERT INTO follow ("uri", "cid", "creator", "subjectDid", "createdAt", "indexedAt")
      SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
      ON CONFLICT DO NOTHING
      RETURNING *
    `,
    toInsert,
  )
  return rows
}

const findDuplicate = async (
  db: DatabaseSchema,
  uri: AtUri,
  obj: Follow.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('follow')
    .where('creator', '=', uri.host)
    .where('subjectDid', '=', obj.subject)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

const notifsForInsert = (obj: IndexedFollow) => {
  return [
    {
      did: obj.subjectDid,
      author: obj.creator,
      recordUri: obj.uri,
      recordCid: obj.cid,
      reason: 'follow' as const,
      reasonSubject: null,
      sortAt: obj.sortAt,
    },
  ]
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedFollow | null> => {
  const deleted = await db
    .deleteFrom('follow')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = (
  deleted: IndexedFollow,
  replacedBy: IndexedFollow | null,
) => {
  const toDelete = replacedBy ? [] : [deleted.uri]
  return { notifs: [], toDelete }
}

const updateAggregates = async (db: DatabaseSchema, follow: IndexedFollow) => {
  const followersCountQb = db
    .insertInto('profile_agg')
    .values({
      did: follow.subjectDid,
      followersCount: db
        .selectFrom('follow')
        .where('follow.subjectDid', '=', follow.subjectDid)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc.column('did').doUpdateSet({
        followersCount: excluded(db, 'followersCount'),
      }),
    )
  const followsCountQb = db
    .insertInto('profile_agg')
    .values({
      did: follow.creator,
      followsCount: db
        .selectFrom('follow')
        .where('follow.creator', '=', follow.creator)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc.column('did').doUpdateSet({
        followsCount: excluded(db, 'followsCount'),
      }),
    )
  await Promise.all([followersCountQb.execute(), followsCountQb.execute()])
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  follows: IndexedFollow[],
) => {
  const followersCountQbs = sql`
    WITH input_values (did) AS (
      SELECT * FROM unnest(${sql`${[follows.map((f) => f.subjectDid)]}::text[]`})
    )
    INSERT INTO profile_agg ("did", "followersCount")
    SELECT
      v.did,
      count(follow.subjectDid) AS followersCount
    FROM
      input_values AS v
      LEFT JOIN follow ON follow.subjectDid = v.did
    GROUP BY v.did
    ON CONFLICT (did) DO UPDATE SET "followersCount" = excluded."followersCount"
  `
  const followsCountQbs = sql`
    WITH input_values (did) AS (
      SELECT * FROM unnest(${sql`${[follows.map((f) => f.creator)]}::text[]`})
    )
    INSERT INTO profile_agg ("did", "followsCount")
    SELECT
      v.did,
      count(follow.creator) AS followsCount
    FROM
      input_values AS v
      LEFT JOIN follow ON follow.creator = v.did
    GROUP BY v.did
    ON CONFLICT (did) DO UPDATE SET "followsCount" = excluded."followsCount"
  `
  await Promise.all([
    followersCountQbs.execute(db),
    followsCountQbs.execute(db),
  ])
}

export type PluginType = RecordProcessor<Follow.Record, IndexedFollow>

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
