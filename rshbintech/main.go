package main

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/gorilla/mux"

	"github.com/jackc/pgx/v4"
)

type BookModel struct {
	Title  string
	Author string
	Cost   int
}

type Service struct {
	Pool   []*pgx.Conn
	IsInit bool
	mux    *sync.Mutex
}

func (s *Service) initService(username, password string) {
	var backgroundTask = func() {
		var databaseUrl = "postgres://" + username + ":" + password + "@10.7.27.34:5432/books"
		for i := 1; i <= 10; i++ {
			conn, err := pgx.Connect(context.Background(), databaseUrl)
			if err != nil {
				println("Ошибка при подключении к базе по URL = " + databaseUrl)
				panic(nil)
			}
			s.Pool = append(s.Pool, conn)
		}
	}

	go backgroundTask()
}

func (s *Service) getBooksByAuthor(username, password string, author *string, result *[]BookModel) error {
	s.mux.Lock()
	if !s.IsInit {
		s.initService(username, password)
		s.IsInit = true
	}
	s.mux.Unlock()

	var conn *pgx.Conn

	s.mux.Lock()
	for _, x := range s.Pool {
		if !x.IsClosed() {
			conn = x
			break
		}
	}
	s.mux.Unlock()

	rows, err := conn.Query(context.Background(), "select title, cost from books where author="+*author)
	if err != nil {
		println("Не удалось получить книги по автору")
		return err
	}

	for rows.Next() {
		var title string
		var cost int
		err = rows.Scan(&title, &cost)
		if err == nil {
			*result = append(*result, BookModel{title, *author, cost})
		}
	}

	println("Успешно выполнен запрос, заполнено записей: " + strconv.Itoa(len(*result)))
	return nil
}

func main() {
	println("Запуск сервера...")
	var service = Service{}

	r := mux.NewRouter()
	r.HandleFunc("/GetBookByAuthor/{author}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		author := vars["author"]
		var result = make([]BookModel, 10)
		service.getBooksByAuthor("boris", "qwerty", &author, &result)
	})
	http.ListenAndServe(":8080", r)
}
