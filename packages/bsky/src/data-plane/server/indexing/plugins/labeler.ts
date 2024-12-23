import { Selectable } from 'kysely'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import * as Labeler from '../../../../lexicon/types/app/bsky/labeler/service'
import * as lex from '../../../../lexicon/lexicons'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import RecordProcessor from '../processor'
import { BackgroundQueue } from '../../background'
import { createCopyWriter } from '../../util'

const lexId = lex.ids.AppBskyLabelerService
type IndexedLabeler = Selectable<DatabaseSchemaType['labeler']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: Labeler.Record,
  timestamp: string,
): Promise<IndexedLabeler | null> => {
  if (uri.rkey !== 'self') return null
  const inserted = await db
    .insertInto('labeler')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
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
//     obj: Labeler.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedLabeler>> => {
//   return db
//     .insertInto('labeler')
//     .values(
//       records.map(({ uri, cid, obj, timestamp }) => ({
//         uri: uri.toString(),
//         cid: cid.toString(),
//         creator: uri.host,
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
    obj: Labeler.Record
    timestamp: string
  }[],
): Promise<Array<IndexedLabeler>> => {
  const inserted: IndexedLabeler[] = []
  const client = await db.pool.connect()
  try {
    const write = createCopyWriter(client, 'labeler', [
      'uri',
      'cid',
      'creator',
      'createdAt',
      'indexedAt',
    ])
    for (const { uri, cid, obj, timestamp } of records) {
      const createdAt = normalizeDatetimeAlways(obj.createdAt)
      const indexedAt = timestamp
      const sortAt =
        new Date(createdAt).getTime() < new Date(indexedAt).getTime()
          ? createdAt
          : indexedAt
      const toInsert = {
        uri: uri.toString(),
        cid: cid.toString(),
        creator: uri.host,
        createdAt,
        indexedAt,
        sortAt,
      }
      inserted.push(toInsert)
      write(toInsert)
    }
  } finally {
    client.release()
  }
  return inserted
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
): Promise<IndexedLabeler | null> => {
  const deleted = await db
    .deleteFrom('labeler')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<Labeler.Record, IndexedLabeler>

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
