#lang racket/base

(require gregor
         net/http-easy
         racket/port
         threading)

(call-with-output-file* (string-append "/var/local/oic/weeklies/weeklyoptions." (~t (today) "yyyy-MM-dd") ".csv")
  (λ (out) (with-handlers ([exn:fail?
                            (λ (error)
                              (displayln (string-append "Encountered error downloading weeklies list"))
                              (displayln error))])
             (~> "https://marketdata.theocc.com/weekly-options?action=download"
                 (get _)
                 (response-body _)
                 (write-bytes _ out))))
  #:exists 'replace)
