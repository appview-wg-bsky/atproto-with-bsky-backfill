import {
  AliasedRawBuilder,
  DummyDriver,
  DynamicModule,
  ExpressionBuilder,
  RawBuilder,
  SelectQueryBuilder,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
  sql,
} from 'kysely'
import { DatabaseSchema, DatabaseSchemaType } from './database-schema'

export const actorWhereClause = (actor: string) => {
  if (actor.startsWith('did:')) {
    return sql<0 | 1>`"actor"."did" = ${actor}`
  } else {
    return sql<0 | 1>`"actor"."handle" = ${actor}`
  }
}

// Applies to actor or record table
export const notSoftDeletedClause = (alias: DbRef) => {
  return sql`${alias}."takedownRef" is null`
}

export const softDeleted = (actorOrRecord: { takedownRef: string | null }) => {
  return actorOrRecord.takedownRef !== null
}

export const countAll = sql<number>`count(*)`

// For use with doUpdateSet()
export const excluded = <T>(db: DatabaseSchema, col) => {
  return sql<T>`${db.dynamic.ref(`excluded.${col}`)}`
}

export const noMatch = sql`1 = 0`

// Can be useful for large where-in clauses, to get the db to use a hash lookup on the list
export const valuesList = (vals: unknown[]) => {
  return sql`(values (${sql.join(vals, sql`), (`)}))`
}

// More advanced version allowing aliasing
// reference: https://kysely.dev/docs/recipes/extending-kysely#a-more-complex-example
export const valuesAliased = <
  R extends Record<string, unknown>,
  A extends string,
>(
  records: R[],
  alias: A,
): AliasedRawBuilder<R, A> => {
  // Assume there's at least one record and all records
  // have the same keys.
  const keys = Object.keys(records[0])

  // Transform the records into a list of lists such as
  // ($1, $2, $3), ($4, $5, $6)
  const values = sql.join(
    records.map((r) => sql`(${sql.join(keys.map((k) => r[k]))})`),
  )

  // Create the alias `v(id, v1, v2)` that specifies the table alias
  // AND a name for each column.
  const wrappedAlias = sql.ref(alias)
  const wrappedColumns = sql.join(keys.map(sql.ref))
  const aliasSql = sql`${wrappedAlias}(${wrappedColumns})`

  // Finally create a single `AliasedRawBuilder` instance of the
  // whole thing. Note that we need to explicitly specify
  // the alias type using `.as<A>` because we are using a
  // raw sql snippet as the alias.
  return sql<R>`(values ${values})`.as<A>(aliasSql)
}

export const dummyDialect = {
  createAdapter() {
    return new SqliteAdapter()
  },
  createDriver() {
    return new DummyDriver()
  },
  createIntrospector(db) {
    return new SqliteIntrospector(db)
  },
  createQueryCompiler() {
    return new SqliteQueryCompiler()
  },
}

export type DbRef = RawBuilder | ReturnType<DynamicModule['ref']>

export type Subquery = ExpressionBuilder<DatabaseSchemaType, any>

export type AnyQb = SelectQueryBuilder<any, any, any>
