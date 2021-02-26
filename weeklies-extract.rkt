#lang racket/base

(require gregor
         net/http-easy
         racket/port
         threading)

(call-with-output-file (string-append "/var/tmp/oic/weeklies/weeklyoptions." (~t (today) "yyyy-MM-dd") ".csv")
  (λ (out) (with-handlers ([exn:fail?
                            (λ (error)
                              (displayln (string-append "Encountered error downloading weeklies list"))
                              (displayln ((error-value->string-handler) error 1000)))])
             (~> "https://marketdata.theocc.com/weekly-options?action=download"
                 (get _)
                 (response-body _)
                 (write-bytes _ out))))
  #:exists 'replace)
