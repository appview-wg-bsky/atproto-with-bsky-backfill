import { Selectable } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { InvalidRequestError } from '@atproto/xrpc-server'
import * as lex from '../../../../lexicon/lexicons'
import * as ListItem from '../../../../lexicon/types/app/bsky/graph/listitem'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import { copyIntoTable } from '../../util'
import RecordProcessor from '../processor'

const lexId = lex.ids.AppBskyGraphListitem
type IndexedListItem = Selectable<DatabaseSchemaType['list_item']>

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: ListItem.Record,
  timestamp: string,
): Promise<IndexedListItem | null> => {
  const listUri = new AtUri(obj.list)
  if (listUri.hostname !== uri.hostname) {
    throw new InvalidRequestError(
      'Creator of listitem does not match creator of list',
    )
  }
  const inserted = await db
    .insertInto('list_item')
    .values({
      uri: uri.toString(),
      cid: cid.toString(),
      creator: uri.host,
      subjectDid: obj.subject,
      listUri: obj.list,
      createdAt: normalizeDatetimeAlways(obj.createdAt),
      indexedAt: timestamp,
    })
    .onConflict((oc) => oc.doNothing())
    .returningAll()
    .executeTakeFirst()
  return inserted || null
}

const findDuplicate = async (
  db: DatabaseSchema,
  _uri: AtUri,
  obj: ListItem.Record,
): Promise<AtUri | null> => {
  const found = await db
    .selectFrom('list_item')
    .where('listUri', '=', obj.list)
    .where('subjectDid', '=', obj.subject)
    .selectAll()
    .executeTakeFirst()
  return found ? new AtUri(found.uri) : null
}

// const insertBulkFn = async (
//   db: DatabaseSchema,
//   records: {
//     uri: AtUri
//     cid: CID
//     obj: ListItem.Record
//     timestamp: string
//   }[],
// ): Promise<Array<IndexedListItem>> => {
//   const toInsert = transpose(records, ({ uri, cid, obj, timestamp }) => [
//     /* uri: */ uri.toString(),
//     /* cid: */ cid.toString(),
//     /* creator: */ uri.host,
//     /* subjectDid: */ obj.subject,
//     /* listUri: */ obj.list,
//     /* createdAt: */ normalizeDatetimeAlways(obj.createdAt),
//     /* indexedAt: */ timestamp,
//   ])
//   return executeRaw<IndexedListItem>(
//     db,
//     `
//     INSERT INTO list_item ("uri", "cid", "creator", "subjectDid", "listUri", "createdAt", "indexedAt")
//     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
//     ON CONFLICT DO NOTHING
//   `,
//     toInsert,
//   )
//     .then((r) => r.rows)
//     .catch((e) => {
//       throw new Error(`Failed to insert list items`, { cause: e })
//     })
// }

const insertBulkFn = async (
  db: Database,
  records: {
    uri: AtUri
    cid: CID
    obj: ListItem.Record
    timestamp: string
  }[],
): Promise<Array<IndexedListItem>> => {
  return copyIntoTable(
    db.pool,
    'list_item',
    [
      'uri',
      'cid',
      'creator',
      'subjectDid',
      'listUri',
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
        subjectDid: obj.subject,
        listUri: obj.list,
        createdAt,
        indexedAt,
        sortAt,
      }
    }),
  )
}

const notifsForInsert = () => {
  return []
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedListItem | null> => {
  const deleted = await db
    .deleteFrom('list_item')
    .where('uri', '=', uri.toString())
    .returningAll()
    .executeTakeFirst()
  return deleted || null
}

const notifsForDelete = () => {
  return { notifs: [], toDelete: [] }
}

export type PluginType = RecordProcessor<ListItem.Record, IndexedListItem>

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
