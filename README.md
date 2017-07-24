# mig

Mig is a simple migration tool for Golang. Migrations in mig are
meant to be embedded in your app. If that's not a good approach
for your app, you can build a *second* app with mig that
does nothing but migrations, or you can just use a different
tool. ;)

## Registering Migrations

The first step is to register your migrations. Call
mig.RegisterMigrations, passing it any number of strings.

```go
func init(){
  mig.RegisterMigrations(
    `
      CREATE TABLE app_user (
        id int NOT NULL,
        ... other columns
      )
    `,
    `ALTER TABLE app_user add column ... `,
    `...`,
  )
}
```

## Running Migrations

You can run all of the registered migrations whenever makes sense
for your app. Once the migrations are run, additional calls to
`mig.RegisterMigrations` will have no effect. It's pretty common
to register migrations in `init` functions so you know everything
is registered by the time `main` runs.

```go
func main(){
  db := sqlx.Connect(...)
  // mig works with sqlx or sql
  err := mig.RunMigrations(db)
  ...
}
```

## Migration Prerequisites

One of the goals of `mig` is to keep the migrations close to the
source code that depends on them. For example, you might have
a `user` package which includes user management logic. That
package would be a good place to put your migrations for your
`app_user` and other related tables.

This works great until you have other tables in other packages
which depend on the app_user table, for example with foreign
keys. Then it's important that the app_user table is created
before those migrations which depend on it are run. For these
cases, you can use prerequisites:

```go
package photos
func init(){
  mig.RegisterMigrations(
    // `mig.Prereq` will cause this sequence of migrations
    // to pause at this point until the query no longer returns
    // an error. Other migration sequences will be given a chance
    // to run before this query is retried.
    mig.Prereq(`select 1 from app_user limit 0`),
    `
      CREATE TABLE photos (
        id int NOT NULL,
        owner_user_id int NOT NULL,

        -- this would fail if the app_user table wasn't created
        -- however, the 'mig.Prereq' query above ensures that
        -- the migration which creates the app_user table
        -- will have run before this migration runs
        FOREIGN KEY (owner_user_id) REFERENCES app_user(id)

        ... other columns
      )
    `,
  )
}


```

## Logging

By default mig is pretty quiet about what it's doing, unless
it encounters an error, in which case it calls log.Fatalf
(which then calls os.Exit(1)).

To customize how logging works, pass a mig.Logger (which is
already implemented by log.Logger) to mig.SetLogger. If you
don't want errors to be fatal, pass a custom type which
implements Fatalf without calling os.Exit.

```go
  mig.SetLogger(log.New(os.Stdout, "migrations:", 0))
  // -- or --
  mig.SetLogger(myCustomLogger)

  // run with whatever logger is set
  mig.RunMigrations(db)
```

## Support

Mig supports mysql and postgres. It may also work with other dbs,
but there's no explicit support for them. Let me know if you are
interested in using mig with another dbms.

Mig also supports both sql and sqlx, as well as any other type
that implements the `mig.DB` interface. (go duck typing!)
