package main

import "fmt"

var logger TransactionLogger

func initializeTransactionLog() error {
	var err error

	logger, err = NewPostgresTransactionLogger(PostgresDBParams{
		host:     "localhost",
		dbName:   "db-name",
		user:     "db-user",
		password: "db-password",
	})
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
				// Retrieve any errors
				// Got a DELETE event!
				// Got a PUT event!
			}
		}
	}

	logger.Run()
	return err
}
