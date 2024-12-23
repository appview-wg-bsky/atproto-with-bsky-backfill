import { Selectable } from 'kysely'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { CID } from 'multiformats/cid'
import * as List from '../../../../lexicon/types/app/bsky/graph/list'
import * as lex from '../../../../lexicon/lexicons'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import RecordProcessor from '../processor'
import { Database } from '../../db'
import { BackgroundQueue } from '../../background'
import { copyIntoTable } from '../../util'

const lexId = lex.ids.AppBskyGraphList
type IndexedList = Selectable<DatabaseSchemaType['list']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: List.Record,
  timestamp: string,
): Promise<IndexedList | null> => {
  const inserted = await db
    .insertInto('list')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      name: obj.name,
      purpose: obj.purpose,
      description: obj.description,
      descriptionFacets: obj.descriptionFacets
        ? JSON.stringify(obj.descriptionFacets)
        : undefined,
      avatarCid: obj.avatar?.ref.toString(),
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
//     obj: List.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedList>> => {
//   return db
//     .insertInto('list')
//     .values(
//       records.map(({ uri, cid, obj, timestamp }) => ({
//         uri: uri.toString(),
//         cid: cid.toString(),
//         creator: uri.host,
//         name: obj.name,
//         purpose: obj.purpose,
//         description: obj.description,
//         descriptionFacets: obj.descriptionFacets
//           ? JSON.stringify(obj.descriptionFacets)
//           : undefined,
//         avatarCid: obj.avatar?.ref.toString(),
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
    obj: List.Record
    timestamp: string
  }[],
): Promise<Array<IndexedList>> => {
  const client = await db.pool.connect()
  try {
    return copyIntoTable(
      client,
      'list',
      [
        'uri',
        'cid',
        'creator',
        'name',
        'purpose',
        'description',
        'descriptionFacets',
        'avatarCid',
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
          name: obj.name,
          purpose: obj.purpose,
          description: obj.description ?? null,
          descriptionFacets: obj.descriptionFacets
            ? JSON.stringify(obj.descriptionFacets)
            : null,
          avatarCid: obj.avatar?.ref.toString() ?? null,
          createdAt,
          indexedAt,
          sortAt,
        }
      }),
    )
  } finally {
    client.release()
  }
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
): Promise<IndexedList | null> => {
  const deleted = await db
    .deleteFrom('list')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<List.Record, IndexedList>

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
