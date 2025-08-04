#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/list
         racket/sequence
         racket/string
         threading)

(define file-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket weeklies-transform-load.rkt"
 #:once-each
 [("-d" "--file-date") date-str
                       "Nasdaq file date. Defaults to today"
                       (file-date (iso8601->date date-str))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define insert-counter 0)

(displayln (string-append "/var/local/oic/weeklies/weeklyoptions." (date->iso8601 (file-date)) ".csv"))

(call-with-input-file (string-append "/var/local/oic/weeklies/weeklyoptions." (date->iso8601 (file-date)) ".csv")
  (λ (in)
    (~> (sequence->list (in-lines in))
        (map (λ (s) (map (λ (s) (string-trim s))
                         (string-split s ",")))
             _)
        (for-each (λ (row)
                    (with-handlers ([exn:fail?
                                     (λ (error)
                                       (displayln (string-append "Encountered error for " (string-join row ",")))
                                       (displayln error))])
                      (query-exec dbc "
insert into oic.weekly (
  act_symbol,
  effective_date,
  last_seen
) values (
  $1,
  $2::text::date,
  $3::text::date
) on conflict (act_symbol) do update set
  effective_date = $2::text::date,
  last_seen = $3::text::date;
"
                                  (case (first row)
                                    [("BRKB") "BRK.B"]
                                    [("RDSA") "RDS.A"]
                                    [else (first row)])
                                  (third row)
                                  (date->iso8601 (file-date))))) _))))

(disconnect dbc)
