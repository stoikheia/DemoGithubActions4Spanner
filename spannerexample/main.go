package main

import (
	"context"
	"fmt"
	"regexp"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
)

type Client struct {
	admin  *database.DatabaseAdminClient
	client *spanner.Client
}

func (c *Client) Close() {
	defer c.admin.Close()
	defer c.client.Close()
}

type Singer struct {
	SingerId   string `spanner:SingerId`
	FirstName  string `spanner:FirstName`
	LastName   string `spanner:LastName`
	SingerInfo string `spanner:SingerInfo`
}

type Album struct {
	SingerId   string `spanner:SingerId`
	AlbumId    string `spanner:AlbumId`
	AlbumTitle string `spanner:AlbumTitle`
}

func createClients(ctx context.Context, db string) (*Client, error) {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}

	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}

	return &Client{
		admin:  adminClient,
		client: dataClient,
	}, nil
}

func createDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, db string, logger *zap.Logger) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", db)
	}
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE TABLE Singers (
         SingerId   STRING(1024) NOT NULL,
         FirstName  STRING(1024),
         LastName   STRING(1024),
         SingerInfo BYTES(MAX)
        ) PRIMARY KEY (SingerId)`,
			`CREATE TABLE Albums (
         SingerId     STRING(1024) NOT NULL,
         AlbumId      STRING(1024) NOT NULL,
         AlbumTitle   STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	logger.Info("Created database", zap.String("db", db))
	return nil
}

func insert(ctx context.Context, client *spanner.Client, logger *zap.Logger) error {
	singerColumns := []string{"SingerId", "FirstName", "LastName"}
	albumColumns := []string{"SingerId", "AlbumId", "AlbumTitle"}

	uuids := make([]string, 10)
	for i := 0; i < 10; i++ {
		u, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		uuids[i] = u.String()
		logger.Info("uuid", zap.Int("index", i), zap.String("UUID", uuids[i]))
	}

	m1 := []*spanner.Mutation{
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{uuids[0], "Marc", "Richards"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{uuids[1], "Catalina", "Smith"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{uuids[2], "Alice", "Trentor"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{uuids[3], "Lea", "Martin"}),
		spanner.InsertOrUpdate("Singers", singerColumns, []interface{}{uuids[4], "David", "Lomond"}),
	}
	m2 := []*spanner.Mutation{
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{uuids[0], uuids[5], "Total Junk"}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{uuids[1], uuids[6], "Go, Go, Go"}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{uuids[0], uuids[7], "Green"}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{uuids[1], uuids[8], "Forever Hold Your Peace"}),
		spanner.InsertOrUpdate("Albums", albumColumns, []interface{}{uuids[2], uuids[9], "Terrified"}),
	}
	_, err := client.Apply(ctx, m1)
	if err != nil {
		return err
	}
	_, err = client.Apply(ctx, m2)
	return err
}

func queryAlbums(ctx context.Context, client *spanner.Client, logger *zap.Logger) ([]*Album, error) {
	stmt := spanner.Statement{SQL: `SELECT SingerId, AlbumId, AlbumTitle FROM Albums`}
	iter := client.Single().Query(ctx, stmt)
	albums := []*Album{}
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return albums, nil
		}
		if err != nil {
			return nil, err
		}
		var singerID string
		var albumID string
		var albumTitle string
		if err := row.Columns(&singerID, &albumID, &albumTitle); err != nil {
			return nil, err
		}

		a := &Album{
			SingerId:   singerID,
			AlbumId:    albumID,
			AlbumTitle: albumTitle,
		}

		albums = append(albums, a)
	}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any

	db := "projects/create-table-test/instances/test-instance/databases/example-db"
	ctx := context.Background()

	client, err := createClients(ctx, db)
	if err != nil {
		logger.Fatal("Create client", zap.String("db", db), zap.Error(err))
	}

	err = createDatabase(ctx, client.admin, db, logger)
	if err != nil {
		code := spanner.ErrCode(err)
		if code != codes.AlreadyExists {
			logger.Fatal("Create database", zap.String("db", db), zap.Error(err))
		}
	}

	err = insert(ctx, client.client, logger)
	if err != nil {
		logger.Fatal("insert records", zap.String("db", db), zap.Error(err))
	}

	albums, err := queryAlbums(ctx, client.client, logger)
	if err != nil {
		logger.Fatal("query records", zap.String("db", db), zap.Error(err))
	}

	for _, a := range albums {
		logger.Info("row",
			zap.String("SingerID", a.SingerId),
			zap.String("AlbumID", a.AlbumId),
			zap.String("AlbumTitle", a.AlbumTitle),
		)
	}

}
