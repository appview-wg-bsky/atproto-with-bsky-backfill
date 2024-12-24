import { Insertable, Selectable, sql } from 'kysely'
import { CID } from 'multiformats/cid'
import { AtUri, normalizeDatetimeAlways } from '@atproto/syntax'
import { jsonStringToLex } from '@atproto/lexicon'
import {
  Record as PostRecord,
  ReplyRef,
} from '../../../../lexicon/types/app/bsky/feed/post'
import { Record as GateRecord } from '../../../../lexicon/types/app/bsky/feed/threadgate'
import { Record as PostgateRecord } from '../../../../lexicon/types/app/bsky/feed/postgate'
import { isMain as isEmbedImage } from '../../../../lexicon/types/app/bsky/embed/images'
import { isMain as isEmbedExternal } from '../../../../lexicon/types/app/bsky/embed/external'
import { isMain as isEmbedRecord } from '../../../../lexicon/types/app/bsky/embed/record'
import { isMain as isEmbedRecordWithMedia } from '../../../../lexicon/types/app/bsky/embed/recordWithMedia'
import {
  isMention,
  isLink,
} from '../../../../lexicon/types/app/bsky/richtext/facet'
import { $Typed } from '../../../../lexicon/util'
import {
  postUriToPostgateUri,
  postUriToThreadgateUri,
  uriToDid,
} from '../../../../util/uris'
import { RecordWithMedia } from '../../../../views/types'
import { parsePostgate } from '../../../../views/util'
import { BackgroundQueue } from '../../background'
import { Database } from '../../db'
import * as lex from '../../../../lexicon/lexicons'
import { DatabaseSchema, DatabaseSchemaType } from '../../db/database-schema'
import RecordProcessor from '../processor'
import { Notification } from '../../db/tables/notification'
import { Database } from '../../db'
import { countAll, excluded } from '../../db/util'
import {
  executeRaw,
  getAncestorsAndSelfQb,
  getDescendentsQb,
  invalidReplyRoot as checkInvalidReplyRoot,
  transpose,
  violatesThreadGate as checkViolatesThreadGate,
} from '../../util'
import { BackgroundQueue } from '../../background'
import { parsePostgate } from '../../../../views/util'
import {
  postUriToThreadgateUri,
  postUriToPostgateUri,
  uriToDid,
} from '../../../../util/uris'
import { copyIntoTable } from '../../util'

type Notif = Insertable<Notification>
type Post = Selectable<DatabaseSchemaType['post']>
type PostEmbedImage = DatabaseSchemaType['post_embed_image']
type PostEmbedExternal = DatabaseSchemaType['post_embed_external']
type PostEmbedRecord = DatabaseSchemaType['post_embed_record']
type PostAncestor = {
  uri: string
  height: number
}
type PostDescendent = {
  uri: string
  depth: number
  cid: string
  creator: string
  sortAt: string
}
type IndexedPost = {
  post: Post
  facets?: { type: 'mention' | 'link'; value: string }[]
  embeds?: (PostEmbedImage[] | PostEmbedExternal | PostEmbedRecord)[]
  ancestors?: PostAncestor[]
  descendents?: PostDescendent[]
  threadgate?: GateRecord
}

const lexId = lex.ids.AppBskyFeedPost

const REPLY_NOTIF_DEPTH = 5

const insertFn = async (
  db: DatabaseSchema,
  uri: AtUri,
  cid: CID,
  obj: PostRecord,
  timestamp: string,
): Promise<IndexedPost | null> => {
  const post = {
    uri: uri.toString(),
    cid: cid.toString(),
    creator: uri.host,
    text: obj.text,
    createdAt: normalizeDatetimeAlways(obj.createdAt),
    replyRoot: obj.reply?.root?.uri || null,
    replyRootCid: obj.reply?.root?.cid || null,
    replyParent: obj.reply?.parent?.uri || null,
    replyParentCid: obj.reply?.parent?.cid || null,
    langs: obj.langs?.length
      ? sql<string[]>`${JSON.stringify(obj.langs)}` // sidesteps kysely's array serialization, which is non-jsonb
      : null,
    tags: obj.tags?.length
      ? sql<string[]>`${JSON.stringify(obj.tags)}` // sidesteps kysely's array serialization, which is non-jsonb
      : null,
    indexedAt: timestamp,
  }
  const [insertedPost] = await Promise.all([
    db
      .insertInto('post')
      .values(post)
      .onConflict((oc) => oc.doNothing())
      .returningAll()
      .executeTakeFirst(),
    db
      .insertInto('feed_item')
      .values({
        type: 'post',
        uri: post.uri,
        cid: post.cid,
        postUri: post.uri,
        originatorDid: post.creator,
        sortAt:
          post.indexedAt < post.createdAt ? post.indexedAt : post.createdAt,
      })
      .onConflict((oc) => oc.doNothing())
      .executeTakeFirst(),
  ])
  if (!insertedPost) {
    return null // Post already indexed
  }

  if (obj.reply) {
    const { invalidReplyRoot, violatesThreadGate } = await validateReply(
      db,
      uri.host,
      obj.reply,
    )
    if (invalidReplyRoot || violatesThreadGate) {
      Object.assign(insertedPost, { invalidReplyRoot, violatesThreadGate })
      await db
        .updateTable('post')
        .where('uri', '=', post.uri)
        .set({ invalidReplyRoot, violatesThreadGate })
        .executeTakeFirst()
    }
  }

  const facets = (obj.facets || [])
    .flatMap((facet) => facet.features)
    .flatMap((feature) => {
      if (isMention(feature)) {
        return {
          type: 'mention' as const,
          value: feature.did,
        }
      }
      if (isLink(feature)) {
        return {
          type: 'link' as const,
          value: feature.uri,
        }
      }
      return []
    })
  // Embed indices
  const embeds: (PostEmbedImage[] | PostEmbedExternal | PostEmbedRecord)[] = []
  const postEmbeds = separateEmbeds(obj.embed)
  for (const postEmbed of postEmbeds) {
    if (isEmbedImage(postEmbed)) {
      const { images } = postEmbed
      const imagesEmbed = images.map((img, i) => ({
        postUri: uri.toString(),
        position: i,
        imageCid: img.image.ref.toString(),
        alt: img.alt,
      }))
      embeds.push(imagesEmbed)
      await db.insertInto('post_embed_image').values(imagesEmbed).execute()
    } else if (isEmbedExternal(postEmbed)) {
      const { external } = postEmbed
      const externalEmbed = {
        postUri: uri.toString(),
        uri: external.uri,
        title: external.title,
        description: external.description,
        thumbCid: external.thumb?.ref.toString() || null,
      }
      embeds.push(externalEmbed)
      await db.insertInto('post_embed_external').values(externalEmbed).execute()
    } else if (isEmbedRecord(postEmbed)) {
      const { record } = postEmbed
      const embedUri = new AtUri(record.uri)
      const recordEmbed = {
        postUri: uri.toString(),
        embedUri: record.uri,
        embedCid: record.cid,
      }
      embeds.push(recordEmbed)
      await db.insertInto('post_embed_record').values(recordEmbed).execute()

      if (embedUri.collection === lex.ids.AppBskyFeedPost) {
        const quote = {
          uri: uri.toString(),
          cid: cid.toString(),
          subject: record.uri,
          subjectCid: record.cid,
          createdAt: normalizeDatetimeAlways(obj.createdAt),
          indexedAt: timestamp,
        }
        await db
          .insertInto('quote')
          .values(quote)
          .onConflict((oc) => oc.doNothing())
          .returningAll()
          .executeTakeFirst()

        const quoteCountQb = db
          .insertInto('post_agg')
          .values({
            uri: record.uri.toString(),
            quoteCount: db
              .selectFrom('quote')
              .where('quote.subjectCid', '=', record.cid.toString())
              .select(countAll.as('count')),
          })
          .onConflict((oc) =>
            oc
              .column('uri')
              .doUpdateSet({ quoteCount: excluded(db, 'quoteCount') }),
          )
        await quoteCountQb.execute()

        const { violatesEmbeddingRules } = await validatePostEmbed(
          db,
          embedUri.toString(),
          uri.toString(),
        )
        Object.assign(insertedPost, {
          violatesEmbeddingRules: violatesEmbeddingRules,
        })
        if (violatesEmbeddingRules) {
          await db
            .updateTable('post')
            .where('uri', '=', insertedPost.uri)
            .set({ violatesEmbeddingRules: violatesEmbeddingRules })
            .executeTakeFirst()
        }
      }
    } else if (isEmbedVideo(postEmbed)) {
	    const { video } = postEmbed
	    const videoEmbed = {
		    postUri: uri.toString(),
		    videoCid: video.ref.toString(),
		    // @NOTE: alt is required for image but not for video on the lexicon.
		    alt: postEmbed.alt ?? null,
	    }
	    embeds.push(videoEmbed)
	    
	    await db.insertInto('post_embed_video').values(videoEmbed).execute()
    }
  }

  const threadgate = await getThreadgateRecord(db, post.replyRoot || post.uri)
  const ancestors = await getAncestorsAndSelfQb(db, {
    uri: post.uri,
    parentHeight: REPLY_NOTIF_DEPTH,
  })
    .selectFrom('ancestor')
    .selectAll()
    .execute()
  const descendents = await getDescendentsQb(db, {
    uri: post.uri,
    depth: REPLY_NOTIF_DEPTH,
  })
    .selectFrom('descendent')
    .innerJoin('post', 'post.uri', 'descendent.uri')
    .selectAll('descendent')
    .select(['cid', 'creator', 'sortAt'])
    .execute()
  return {
    post: insertedPost,
    facets,
    embeds,
    ancestors,
    descendents,
    threadgate,
  }
}

const insertBulkFn = async (
  db: Database,
  records: Array<{
    uri: AtUri
    cid: CID
    obj: PostRecord
    timestamp: string
  }>,
): Promise<IndexedPost[]> => {
  const client = await db.pool.connect()

  const [insertedPosts] = await Promise.all([
    copyIntoTable(
      client,
      'post',
      [
        'uri',
        'cid',
        'creator',
        'text',
        'createdAt',
        'replyRoot',
        'replyRootCid',
        'replyParent',
        'replyParentCid',
        'langs',
        'tags',
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
          text: obj.text,
          createdAt,
          replyRoot: obj.reply?.root.uri ?? null,
          replyRootCid: obj.reply?.root.cid ?? null,
          replyParent: obj.reply?.parent.uri ?? null,
          replyParentCid: obj.reply?.parent.cid ?? null,
          langs: obj.langs?.length ? obj.langs : null,
          tags: obj.tags?.length ? obj.tags : null,
          indexedAt,
          sortAt,
        }
      }),
    ),
    copyIntoTable(
      client,
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
          postUri: uri.toString(),
          originatorDid: uri.host,
          sortAt,
        }
      }),
    ),
  ])
  if (!insertedPosts.length) {
    return []
  }

  const invalidReplyUpdates = await Promise.all(
    insertedPosts.map(async (insertedPost) => {
      if (insertedPost.replyParent || insertedPost.replyRoot) {
        const record = records.find((r) => r.uri.href === insertedPost.uri)?.obj
        if (!record?.reply) return

        const { invalidReplyRoot, violatesThreadGate } = await validateReply(
          db.db,
          insertedPost.creator,
          record.reply,
        )

        if (invalidReplyRoot || violatesThreadGate) {
          Object.assign(insertedPost, { invalidReplyRoot, violatesThreadGate })
          return {
            uri: insertedPost.uri,
            invalidReplyRoot,
            violatesThreadGate,
          }
        }
      }
    }),
  )

  const toInsertInvalidReplyUpdates = transpose(
    invalidReplyUpdates.filter((u) => !!u),
    ({ uri, invalidReplyRoot, violatesThreadGate }) => [
      uri,
      invalidReplyRoot,
      violatesThreadGate,
    ],
  )

  const invalidReplyUpdatesQuery = executeRaw(
    db.db,
    `
    UPDATE post SET "invalidReplyRoot" = v."invalidReplyRoot", "violatesThreadGate" = v."violatesThreadGate"
    FROM (
      SELECT * FROM unnest($1::text[], $2::boolean[], $3::boolean[]) AS t(uri, "invalidReplyRoot", "violatesThreadGate")
    ) as v
    WHERE post.uri = v.uri
  `,
    toInsertInvalidReplyUpdates,
  )

  const insertRows: {
    post_embed_image?: Record<
      keyof DatabaseSchemaType['post_embed_image'],
      string | null | undefined
    >[]
    post_embed_external?: Record<
      keyof DatabaseSchemaType['post_embed_external'],
      string | null | undefined
    >[]
    post_embed_record?: Record<
      keyof DatabaseSchemaType['post_embed_record'],
      string | null | undefined
    >[]
    quote?: Record<
      keyof DatabaseSchemaType['quote'],
      string | null | undefined
    >[]
    post_agg_quotedPosts?: Map<string, string>
    post_updates_violatesEmbeddingRules?: {
      uri: string
      violatesEmbeddingRules: boolean
    }[]
  } = {}
  for (const post of insertedPosts) {
    const postRecord = records.find((r) => r.uri.toString() === post.uri)
    if (!postRecord) continue
    const obj = postRecord.obj

    const postEmbeds = separateEmbeds(obj.embed)
    for (const postEmbed of postEmbeds) {
      if (isEmbedImage(postEmbed)) {
        const { images } = postEmbed
        const imagesEmbed = images
          .map((img, i) => {
            const imageCid = img.image?.ref?.toString?.()
            if (!imageCid) return
            return {
              postUri: post.uri,
              position: `${i}`,
              imageCid,
              alt: img.alt,
            }
          })
          .filter((e) => !!e)
        insertRows.post_embed_image ??= []
        insertRows.post_embed_image.push(...imagesEmbed)
      } else if (isEmbedExternal(postEmbed)) {
        const { external } = postEmbed
        const externalEmbed = {
          postUri: post.uri,
          uri: external.uri,
          title: external.title,
          description: external.description,
          thumbCid: external.thumb?.ref.toString() ?? null,
        }
        insertRows.post_embed_external ??= []
        insertRows.post_embed_external.push(externalEmbed)
      } else if (isEmbedRecord(postEmbed)) {
        const { record } = postEmbed
        const embedUri = new AtUri(record.uri)
        const recordEmbed = {
          postUri: post.uri,
          embedUri: record.uri,
          embedCid: record.cid,
        }
        insertRows.post_embed_record ??= []
        insertRows.post_embed_record.push(recordEmbed)

        if (embedUri.collection === lex.ids.AppBskyFeedPost) {
          const createdAt = normalizeDatetimeAlways(post.createdAt)
          const indexedAt = post.indexedAt
          const sortAt =
            new Date(createdAt).getTime() < new Date(indexedAt).getTime()
              ? createdAt
              : indexedAt
          const quote = {
            uri: post.uri,
            cid: post.cid,
            subject: record.uri,
            subjectCid: record.cid,
            createdAt,
            indexedAt,
            sortAt,
          }
          insertRows.quote ??= []
          insertRows.quote.push(quote)

          insertRows.post_agg_quotedPosts ??= new Map()
          insertRows.post_agg_quotedPosts.set(record.cid, record.uri)

          const { violatesEmbeddingRules } = await validatePostEmbed(
            db.db,
            embedUri.toString(),
            post.uri,
          )
          Object.assign(post, { violatesEmbeddingRules })
          if (violatesEmbeddingRules) {
            insertRows.post_updates_violatesEmbeddingRules ??= []
            insertRows.post_updates_violatesEmbeddingRules.push({
              uri: post.uri,
              violatesEmbeddingRules,
            })
          }
        }
      }
    }
  }

  const toInsertViolatesEmbeddingRules = transpose(
    insertRows.post_updates_violatesEmbeddingRules ?? [],
    (v) => [v.uri, v.violatesEmbeddingRules],
  )
  const violatesEmbeddingRulesQuery =
    insertRows.post_updates_violatesEmbeddingRules?.length &&
    executeRaw(
      db.db,
      `
      UPDATE post SET "violatesEmbeddingRules" = v."violatesEmbeddingRules"
      FROM (
        SELECT * FROM unnest($1::text[], $2::boolean[]) AS t(uri, "violatesEmbeddingRules")
      ) as v
      WHERE post.uri = v.uri
    `,
      toInsertViolatesEmbeddingRules,
    )

  await Promise.all([
    invalidReplyUpdatesQuery,
    insertRows.post_embed_image &&
      copyIntoTable(
        client,
        'post_embed_image',
        ['postUri', 'position', 'imageCid', 'alt'],
        insertRows.post_embed_image,
      ),
    insertRows.post_embed_external &&
      copyIntoTable(
        client,
        'post_embed_external',
        ['postUri', 'uri', 'title', 'description', 'thumbCid'],
        insertRows.post_embed_external,
      ),
    insertRows.post_embed_record &&
      copyIntoTable(
        client,
        'post_embed_record',
        ['postUri', 'embedUri', 'embedCid'],
        insertRows.post_embed_record,
      ),
    insertRows.quote &&
      copyIntoTable(
        client,
        'quote',
        [
          'uri',
          'cid',
          'subject',
          'subjectCid',
          'createdAt',
          'indexedAt',
          'sortAt',
        ],
        insertRows.quote,
      ),
    insertRows.post_agg_quotedPosts?.size &&
      db.db
        .insertInto('post_agg')
        .columns(['uri', 'quoteCount'])
        .expression((eb) =>
          eb
            .selectFrom('quote')
            .where(
              'quote.subjectCid',
              'in',
              Array.from(insertRows.post_agg_quotedPosts!.keys()),
            )
            .groupBy(['subjectCid', 'subject'])
            .select(['subject as uri', countAll.as('quoteCount')]),
        )
        .onConflict((oc) =>
          oc
            .column('uri')
            .doUpdateSet({ quoteCount: excluded(db.db, 'quoteCount') }),
        )
        .execute()
        .catch((e) => {
          throw new Error('Failed to update aggregates', { cause: e })
        }),
    violatesEmbeddingRulesQuery,
  ])

  return insertedPosts.map((post) => ({ post }))
}

const findDuplicate = async (): Promise<AtUri | null> => {
  return null
}

const notifsForInsert = (obj: IndexedPost) => {
  const notifs: Notif[] = []
  const notified = new Set([obj.post.creator])
  const maybeNotify = (notif: Notif) => {
    if (!notified.has(notif.did)) {
      notified.add(notif.did)
      notifs.push(notif)
    }
  }
  for (const facet of obj.facets ?? []) {
    if (facet.type === 'mention') {
      maybeNotify({
        did: facet.value,
        reason: 'mention',
        author: obj.post.creator,
        recordUri: obj.post.uri,
        recordCid: obj.post.cid,
        sortAt: obj.post.sortAt,
      })
    }
  }

  if (!obj.post.violatesEmbeddingRules) {
    for (const embed of obj.embeds ?? []) {
      if ('embedUri' in embed) {
        const embedUri = new AtUri(embed.embedUri)
        if (embedUri.collection === lex.ids.AppBskyFeedPost) {
          maybeNotify({
            did: embedUri.host,
            reason: 'quote',
            reasonSubject: embedUri.toString(),
            author: obj.post.creator,
            recordUri: obj.post.uri,
            recordCid: obj.post.cid,
            sortAt: obj.post.sortAt,
          })
        }
      }
    }
  }

  if (obj.post.violatesThreadGate) {
    // don't generate reply notifications when post violates threadgate
    return notifs
  }

  const threadgateHiddenReplies = obj.threadgate?.hiddenReplies || []

  // reply notifications

  for (const ancestor of obj.ancestors ?? []) {
    if (ancestor.uri === obj.post.uri) continue // no need to notify for own post
    if (ancestor.height < REPLY_NOTIF_DEPTH) {
      const ancestorUri = new AtUri(ancestor.uri)
      maybeNotify({
        did: ancestorUri.host,
        reason: 'reply',
        reasonSubject: ancestorUri.toString(),
        author: obj.post.creator,
        recordUri: obj.post.uri,
        recordCid: obj.post.cid,
        sortAt: obj.post.sortAt,
      })
      // found hidden reply, don't notify any higher ancestors
      if (threadgateHiddenReplies.includes(ancestorUri.toString())) break
    }
  }

  // descendents indicate out-of-order indexing: need to notify
  // the current post and upwards.
  for (const descendent of obj.descendents ?? []) {
    for (const ancestor of obj.ancestors ?? []) {
      const totalHeight = descendent.depth + ancestor.height
      if (totalHeight < REPLY_NOTIF_DEPTH) {
        const ancestorUri = new AtUri(ancestor.uri)
        maybeNotify({
          did: ancestorUri.host,
          reason: 'reply',
          reasonSubject: ancestorUri.toString(),
          author: descendent.creator,
          recordUri: descendent.uri,
          recordCid: descendent.cid,
          sortAt: descendent.sortAt,
        })
      }
    }
  }

  return notifs
}

const deleteFn = async (
  db: DatabaseSchema,
  uri: AtUri,
): Promise<IndexedPost | null> => {
  const uriStr = uri.toString()
  const [deleted] = await Promise.all([
    db
      .deleteFrom('post')
      .where('uri', '=', uriStr)
      .returningAll()
      .executeTakeFirst(),
    db.deleteFrom('feed_item').where('postUri', '=', uriStr).executeTakeFirst(),
  ])
  await db.deleteFrom('quote').where('subject', '=', uriStr).execute()
  const deletedEmbeds: (
    | PostEmbedImage[]
    | PostEmbedExternal
    | PostEmbedRecord
  )[] = []
  const [deletedImgs, deletedExternals, deletedPosts] = await Promise.all([
    db
      .deleteFrom('post_embed_image')
      .where('postUri', '=', uriStr)
      .returningAll()
      .execute(),
    db
      .deleteFrom('post_embed_external')
      .where('postUri', '=', uriStr)
      .returningAll()
      .executeTakeFirst(),
    db
      .deleteFrom('post_embed_record')
      .where('postUri', '=', uriStr)
      .returningAll()
      .executeTakeFirst(),
  ])
  if (deletedImgs.length) {
    deletedEmbeds.push(deletedImgs)
  }
  if (deletedExternals) {
    deletedEmbeds.push(deletedExternals)
  }
  if (deletedPosts) {
    const embedUri = new AtUri(deletedPosts.embedUri)
    deletedEmbeds.push(deletedPosts)

    if (embedUri.collection === lex.ids.AppBskyFeedPost) {
      await db.deleteFrom('quote').where('uri', '=', uriStr).execute()
      await db
        .insertInto('post_agg')
        .values({
          uri: deletedPosts.embedUri,
          quoteCount: db
            .selectFrom('quote')
            .where('quote.subjectCid', '=', deletedPosts.embedCid.toString())
            .select(countAll.as('count')),
        })
        .onConflict((oc) =>
          oc
            .column('uri')
            .doUpdateSet({ quoteCount: excluded(db, 'quoteCount') }),
        )
        .execute()
    }
  }
  return deleted
    ? {
        post: deleted,
        facets: [], // Not used
        embeds: deletedEmbeds,
      }
    : null
}

const notifsForDelete = (
  deleted: IndexedPost,
  replacedBy: IndexedPost | null,
) => {
  const notifs = replacedBy ? notifsForInsert(replacedBy) : []
  return {
    notifs,
    toDelete: [deleted.post.uri],
  }
}

const updateAggregates = async (db: DatabaseSchema, postIdx: IndexedPost) => {
  const replyCountQb = postIdx.post.replyParent
    ? db
        .insertInto('post_agg')
        .values({
          uri: postIdx.post.replyParent,
          replyCount: db
            .selectFrom('post')
            .where('post.replyParent', '=', postIdx.post.replyParent)
            .where((qb) =>
              qb
                .where('post.violatesThreadGate', 'is', null)
                .orWhere('post.violatesThreadGate', '=', false),
            )
            .select(countAll.as('count')),
        })
        .onConflict((oc) =>
          oc
            .column('uri')
            .doUpdateSet({ replyCount: excluded(db, 'replyCount') }),
        )
    : null
  const postsCountQb = db
    .insertInto('profile_agg')
    .values({
      did: postIdx.post.creator,
      postsCount: db
        .selectFrom('post')
        .where('post.creator', '=', postIdx.post.creator)
        .select(countAll.as('count')),
    })
    .onConflict((oc) =>
      oc.column('did').doUpdateSet({ postsCount: excluded(db, 'postsCount') }),
    )
  await Promise.all([replyCountQb?.execute(), postsCountQb.execute()])
}

const updateAggregatesBulk = async (
  db: DatabaseSchema,
  posts: IndexedPost[],
) => {
  const replyCountQbs = sql`
    WITH input_values (uri) AS (
        SELECT * FROM unnest(${sql`${[
          posts
            .filter((p) => p.post.replyParent)
            .map((p) => p.post.replyParent),
        ]}`}::text[])
    )
    INSERT INTO post_agg ("uri", "replyCount")
    SELECT
      v.uri,
      count(post."replyParent") AS replyCount
    FROM
      input_values AS v
      LEFT JOIN post ON post."replyParent" = v.uri
        AND (post."violatesThreadGate" IS NULL OR post."violatesThreadGate" = false)
    GROUP BY v.uri
    ON CONFLICT (uri) DO UPDATE SET "replyCount" = excluded."replyCount"
  `
  const postsCountQbs = sql`
    WITH input_values (did) AS (
        SELECT * FROM unnest(${sql`${posts.map((p) => p.post.creator)}::text[]`})
    )
    INSERT INTO profile_agg ("did", "postsCount")
    SELECT
      v.did,
      count(post.creator) AS postsCount
    FROM
      input_values AS v
      LEFT JOIN post ON post.creator = v.did
    GROUP BY v.did
    ON CONFLICT (did) DO UPDATE SET "postsCount" = excluded."postsCount"
  `

  await Promise.all([
    replyCountQbs.execute(db).catch((e) => {
      throw new Error('Failed to update reply count aggregate', { cause: e })
    }),

    postsCountQbs.execute(db).catch((e) => {
      throw new Error('Failed to update posts count aggregate', { cause: e })
    }),
  ])
}

export type PluginType = RecordProcessor<PostRecord, IndexedPost>

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

function separateEmbeds(
  embed: PostRecord['embed'],
): Array<
  | RecordWithMedia['media']
  | $Typed<RecordWithMedia['record']>
  | NonNullable<PostRecord['embed']>
> {
  if (!embed) {
    return []
  }
  if (isEmbedRecordWithMedia(embed)) {
    return [{ $type: lex.ids.AppBskyEmbedRecord, ...embed.record }, embed.media]
  }
  return [embed]
}

async function validateReply(
  db: DatabaseSchema,
  creator: string,
  reply: ReplyRef,
) {
  const replyRefs = await getReplyRefs(db, reply)
  // check reply
  const invalidReplyRoot =
    !replyRefs.parent || checkInvalidReplyRoot(reply, replyRefs.parent)
  // check interaction
  const violatesThreadGate = await checkViolatesThreadGate(
    db,
    creator,
    uriToDid(reply.root.uri),
    replyRefs.root?.record ?? null,
    replyRefs.gate?.record ?? null,
  )
  return {
    invalidReplyRoot,
    violatesThreadGate,
  }
}

async function getThreadgateRecord(db: DatabaseSchema, postUri: string) {
  const threadgateRecordUri = postUriToThreadgateUri(postUri)
  const results = await db
    .selectFrom('record')
    .where('record.uri', '=', threadgateRecordUri)
    .selectAll()
    .execute()
  const threadgateRecord = results.find(
    (ref) => ref.uri === threadgateRecordUri,
  )
  if (threadgateRecord) {
    return jsonStringToLex(threadgateRecord.json) as GateRecord
  }
}

async function validatePostEmbed(
  db: DatabaseSchema,
  embedUri: string,
  parentUri: string,
) {
  const postgateRecordUri = postUriToPostgateUri(embedUri)
  const postgateRecord = await db
    .selectFrom('record')
    .where('record.uri', '=', postgateRecordUri)
    .selectAll()
    .executeTakeFirst()
  if (!postgateRecord) {
    return {
      violatesEmbeddingRules: false,
    }
  }
  const {
    embeddingRules: { canEmbed },
  } = parsePostgate({
    gate: jsonStringToLex(postgateRecord.json) as PostgateRecord,
    viewerDid: uriToDid(parentUri),
    authorDid: uriToDid(embedUri),
  })
  if (canEmbed) {
    return {
      violatesEmbeddingRules: false,
    }
  }
  return {
    violatesEmbeddingRules: true,
  }
}

async function getReplyRefs(db: DatabaseSchema, reply: ReplyRef) {
  const replyRoot = reply.root.uri
  const replyParent = reply.parent.uri
  const replyGate = postUriToThreadgateUri(replyRoot)
  const results = await db
    .selectFrom('record')
    .where('record.uri', 'in', [replyRoot, replyGate, replyParent])
    .leftJoin('post', 'post.uri', 'record.uri')
    .selectAll('post')
    .select(['record.uri', 'json'])
    .execute()
  const root = results.find((ref) => ref.uri === replyRoot)
  const parent = results.find((ref) => ref.uri === replyParent)
  const gate = results.find((ref) => ref.uri === replyGate)
  return {
    root: root && {
      uri: root.uri,
      invalidReplyRoot: root.invalidReplyRoot,
      record: jsonStringToLex(root.json) as PostRecord,
    },
    parent: parent && {
      uri: parent.uri,
      invalidReplyRoot: parent.invalidReplyRoot,
      record: jsonStringToLex(parent.json) as PostRecord,
    },
    gate: gate && {
      uri: gate.uri,
      record: jsonStringToLex(gate.json) as GateRecord,
    },
  }
}
