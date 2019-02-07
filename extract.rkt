#lang racket/base

(require db
         net/url
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         srfi/19 ; Time Data Types and Procedures
         tasks
         threading)

(define (download-options-chains symbol session-id)
  (make-directory* (string-append "/var/tmp/oic/options-chains/" (date->string (current-date) "~1")))
  (call-with-output-file (string-append "/var/tmp/oic/options-chains/" (date->string (current-date) "~1") "/" symbol ".html")
    (λ (out) (with-handlers ([exn:fail:network:errno
                              (λ (errno error)
                                (displayln (string-append "Encountered network error for " symbol))
                                (displayln ((error-value->string-handler) error 1000)))])
               (~> (string-append "https://oic.ivolatility.com/oic_adv_options.j;jsessionid=" session-id
                                  "?ticker=" (string-replace symbol "." "/") "&exp_date=-1")
                   (string->url _)
                   (get-pure-port _)
                   (copy-port _ out))))
    #:exists 'replace))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define j-session-id (make-parameter ""))

(command-line
 #:program "racket extract.rkt"
 #:once-each
 [("-j" "--j-session-id") session-id
                          "J Session Id"
                          (j-session-id session-id)]
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

(define sp500-symbols (query-list dbc "
select
  component_symbol
from
  spdr.etf_holding
where
  etf_symbol = 'SPY' and
  component_symbol != 'LIN' and
  date = (select max(date) from spdr.etf_holding)
order by
  component_symbol;
"))

(disconnect dbc)

(define delay-interval 10)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length sp500-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (download-options-chains (first l) (j-session-id)))
                                                          (second l)))
                            (map list sp500-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
