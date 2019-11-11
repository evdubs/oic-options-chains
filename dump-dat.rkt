#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/vector)

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dump-dat.rkt"
 #:once-each
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define (vector->csv-line vec)
  (if (= 1 (vector-length vec))
      (vector-ref vec 0)
      (string-append (vector-ref vec 0) "," (vector->csv-line (vector-drop vec 1)))))

(for-each (λ (date)
            (call-with-output-file (string-append "/var/tmp/dat/oic/option-chain/" date ".csv")
              (λ (out)
                (displayln "act_symbol,expiration,strike,call_put,date,bid,ask,vol,delta,gamma,theta,vega,rho" out)
                (for-each (λ (row)
                            (displayln (vector->csv-line row) out))
                          (query-rows dbc "
select
  act_symbol::text,
  expiration::text,
  strike::text,
  call_put::text,
  date::text,
  bid::text,
  ask::text,
  vol::text,
  delta::text,
  gamma::text,
  theta::text,
  vega::text,
  rho::text
from
  oic.option_chain
where
  date = $1::text::date and
  bid is not null and
  ask is not null and
  vol is not null and
  delta is not null and
  gamma is not null and
  theta is not null and
  vega is not null and
  rho is not null
order by
  act_symbol, expiration, strike, call_put, date;
"
                                      date)))
              #:exists 'replace))
          (query-list dbc "
select distinct
  date::text
from
  oic.option_chain
where
  date >= $1::text::date and
  date <= $2::text::date
order by
  date;
"
                      (start-date)
                      (end-date)))

