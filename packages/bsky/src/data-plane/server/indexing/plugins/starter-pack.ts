import { Selectable } from 'kysely'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import * as StarterPack from '../../../../lexicon/types/app/bsky/graph/starterpack'
import * as lex from '../../../../lexicon/lexicons'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import RecordProcessor from '../processor'
import { BackgroundQueue } from '../../background'
import { copyIntoTable } from '../../util'

const lexId = lex.ids.AppBskyGraphStarterpack
type IndexedStarterPack = Selectable<DatabaseSchemaType['starter_pack']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: StarterPack.Record,
  timestamp: string,
): Promise<IndexedStarterPack | null> => {
  const inserted = await db
    .insertInto('starter_pack')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      name: obj.name,
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
//     obj: StarterPack.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedStarterPack>> => {
//   return db
//     .insertInto('starter_pack')
//     .values(
//       records.map(({ uri, cid, obj, timestamp }) => ({
//         uri: uri.toString(),
//         cid: cid.toString(),
//         creator: uri.host,
//         name: obj.name,
//         createdAt: normalizeDatetimeAlways(obj.createdAt),
//         indexedAt: timestamp,
//       })),
//     )
//     .onConflict((oc) => oc.doNothing())
//     .returningAll()
//     .execute()
// }

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: StarterPack.Record
    timestamp: string
  }[],
): Promise<Array<IndexedStarterPack>> => {
  return copyIntoTable(
    db.pool,
    'starter_pack',
    ['uri', 'cid', 'creator', 'name', 'createdAt', 'indexedAt'],
    records.map(({ uri, cid, obj, timestamp }) => {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      return {
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        name: obj.name,
        createdAt,
        indexedAt,
      }
    }),
  )
}

const findDuplicate = async (): Promise<AtUri | null> => {
  return null
}

const notifsForInsert = () => {
  return []
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedStarterPack | null> => {
  const deleted = await db
    .deleteFrom('starter_pack')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<StarterPack.Record, IndexedStarterPack>

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
  })
}

export default makePlugin
