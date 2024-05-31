#lang racket/base

(require db
         gregor
         html-parsing
         json
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/match
         racket/set
         racket/string
         sxml
         tasks
         threading)

(define email-address (make-parameter ""))

(define oic-password (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(command-line
 #:program "racket extract.2023-11-16.rkt"
 #:once-each
 [("-e" "--email-address") email
                           "Email address used for OIC"
                           (email-address email)]
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)]
 [("-s" "--secret-password") pass
                             "Password used for OIC"
                             (oic-password pass)])

(define (get-bearer-token)
  (with-handlers ([exn:fail?
                   (λ (error)
                     (displayln (string-append "Encountered error while refreshing bearer-token."))
                     (displayln error)
                     bearer-token)])
    (define site-rsp (get "https://www.optionseducation.org"
                          #:max-redirects 0))

    (define site-cookies (foldl string-append "" (map (λ (bs) (string-append (bytes->string/utf-8 bs) "; "))
                                                      (response-headers-ref* site-rsp 'Set-Cookie))))

    (define site-xexp (~> (response-body site-rsp)
                          (bytes->string/utf-8 _)
                          (html->xexp _)))

    (define data-token
      (cadr (first ((sxpath '(html body div div div @ data-token)) site-xexp))))
    
    (define login-headers
      (response-headers (post "https://www.optionseducation.org/api/account/login"
                              #:max-redirects 0
                              #:headers (hash 'Cookie site-cookies
                                              'X-XSRF-TOKEN data-token)
                              #:json (hash 'Email (email-address)
                                           'Password (oic-password)))))
    
    (define identity-auth
      (filter-map (λ (h) (match h [(pregexp #px"identity\\.authentication=([0-9a-zA-Z\\-_]+);" (list str auth))
                                   (bytes->string/utf-8 auth)]
                                [_ #f]))
                  login-headers))

    (define options-monitor-xexp
      (~> (get "https://www.optionseducation.org/toolsoptionquotes/options-monitor"
               #:headers (hash 'Cookie (string-append "identity.authentication="
                                                      (first identity-auth))))
          (response-body _)
          (bytes->string/utf-8 _)
          (html->xexp)))

    (define new-data-token
      (cadr (first ((sxpath '(html body div main div (div 1) @ data-token)) options-monitor-xexp))))

    ; 2024-05-15 not sure why this part is no longer needed. this may be related to being forced to
    ; reset the password
    ;
    ;(define data-fetch
    ;  (cadr (first ((sxpath '(html body div main div (div 1) @ data-fetch)) options-monitor-xexp))))
    ;(define data-name
    ;  (cadr (first ((sxpath '(html body div main div (div 1) @ data-name)) options-monitor-xexp))))
    ;(define data-key
    ;  (cadr (first ((sxpath '(html body div main div (div 1) @ data-key)) options-monitor-xexp))))
    ;(bytes->string/utf-8 (response-body (post data-fetch
    ;                                          #:json (hash 'clientKey data-key
    ;                                                       'clientName data-name))))

    new-data-token))

(define bearer-token (get-bearer-token))

(define symbol-info
  (~> (get "https://private-authorization.ivolatility.com/lookup/?region=1&matchingType=CONTAINS&sortField=SYMBOL"
           #:headers (hash 'Authorization (string-append "Bearer " bearer-token)))
      (response-body _)
      (bytes->jsexpr _)))

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define symbols (query-list dbc "
select distinct
  component_symbol as symbol
from
  spdr.etf_holding
where
  etf_symbol in ('SPY', 'MDY', 'SLY', 'SPSM') and
  date = (select max(date) from spdr.etf_holding) and
  case when $1 != ''
    then component_symbol >= $1
    else true
  end and
  case when $2 != ''
    then component_symbol <= $2
    else true
  end
union
select distinct
  etf_symbol as symbol
from
  spdr.etf_holding
where
  date = (select max(date) from spdr.etf_holding) and
  case when $1 != ''
    then etf_symbol >= $1
    else true
  end and
  case when $2 != ''
    then etf_symbol <= $2
    else true
  end
order by
  symbol;
"
                            (first-symbol)
                            (last-symbol)))

(define symbol-set (list->set symbols))

(disconnect dbc)

(define symbol-ids
  (~> (filter-map (λ (s)
                    (cond [(set-member? symbol-set (string-replace (hash-ref s 'symbol) "/" "."))
                           (cons (string-replace (hash-ref s 'symbol) "/" ".") (hash-ref s 'stockId))]
                          [else #f]))
                  (hash-ref symbol-info 'page))
      (make-immutable-hash _)))

(define (download-options-chains symbol symbol-id)
  (make-directory* (string-append "/var/tmp/oic/options-chains/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file* (string-append "/var/tmp/oic/options-chains/" (~t (today) "yyyy-MM-dd") "/" symbol ".json")
    (λ (out) (with-handlers ([exn:fail?
                              (λ (error)
                                (displayln (string-append "Encountered error for " symbol))
                                (displayln error))])
               (~> (get (string-append "https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?stockId="
                                       (number->string symbol-id)
                                       "&center=0&columns=strike&columns=bid&columns=ask&columns=iv&columns=ivint"
                                       "&columns=delta&columns=gamma&columns=theta&columns=vega&columns=rho")
                        #:headers (hash 'Authorization (string-append "Bearer " bearer-token))
                        #:timeouts (make-timeout-config #:request 120))
                   (response-body _)
                   (write-bytes _ out))))
    #:exists 'replace))

(define (get-cnt)
  (with-handlers ([exn:fail?
                   (λ (error)
                     (displayln (string-append "Encountered error while refreshing cnt."))
                     (displayln error)
                     cnt)])
    (define site-rsp (get "https://www.optionseducation.org"
                          #:max-redirects 0))

    (define site-cookies (foldl string-append "" (map (λ (bs) (string-append (bytes->string/utf-8 bs) "; "))
                                                      (response-headers-ref* site-rsp 'Set-Cookie))))

    (define site-xexp (~> (response-body site-rsp)
                          (bytes->string/utf-8 _)
                          (html->xexp _)))

    (define data-token
      (cadr (first ((sxpath '(html body div div div @ data-token)) site-xexp))))
    
    (define login-headers
      (response-headers (post "https://www.optionseducation.org/api/account/login"
                              #:max-redirects 0
                              #:headers (hash 'Cookie site-cookies
                                              'X-XSRF-TOKEN data-token)
                              #:json (hash 'Email (email-address)
                                           'Password (oic-password)))))
    
    (define identity-auth
      (filter-map (λ (h) (match h [(pregexp #px"identity\\.authentication=([0-9a-zA-Z\\-_]+);" (list str auth))
                                   (bytes->string/utf-8 auth)]
                                [_ #f]))
                  login-headers))
    
    (~> (get "https://www.optionseducation.org/toolsoptionquotes/historical-and-implied-volatility"
             #:headers (hash 'Cookie (string-append "identity.authentication="
                                                    (first identity-auth))))
        (response-body _)
        (bytes->string/utf-8 _)
        (regexp-match #rx"cnt=([A-F0-9]+)" _)
        (second _))))

; (define cnt (get-cnt))
(define cnt "")

(define (download-volatility symbol)
  (make-directory* (string-append "/var/tmp/oic/options-chains/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file* (string-append "/var/tmp/oic/options-chains/" (~t (today) "yyyy-MM-dd") "/" symbol ".html")
    (λ (out) (with-handlers ([exn:fail?
                              (λ (error)
                                (displayln (string-append "Encountered error for " symbol))
                                (displayln error))])
               (~> (string-append "https://occ.ivolatility.com/oic_options.j" ; "?cnt=" cnt
                                  "?ticker=" (string-replace symbol "." "/") "&exp_date=-1")
                   (get _ #:timeouts (make-timeout-config #:request 120))
                   (response-body _)
                   (write-bytes _ out))))
    #:exists 'replace))

(define delay-interval 20)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length symbols))))

(with-task-server
  (for-each (λ (l) (schedule-delayed-task
                    (λ () (cond [(= 0 (modulo (second l) 1800)) (thread (λ () (set! bearer-token (get-bearer-token))
                                                                           ;(set! cnt (get-cnt))
                                                                           ))])
                       (thread (λ () (download-options-chains (first l) (hash-ref symbol-ids (first l)))
                                  (download-volatility (first l)))))
                    (second l)))
            (map list symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
