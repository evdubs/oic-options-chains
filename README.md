# oic-options-chains
These Racket programs will download the OIC Options Chains HTML files and insert the data into a PostgreSQL database. The intended usage is:

```
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a /var/tmp/oic/options-chains folder. This process also assumes you have loaded your database with the NASDAQ symbol file information and SPDR ETF Holding information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project and [spdr-etf-holdings](https://github.com/evdubs/spdr-etf-holdings) project. These programs currently (2019-02-06) will just extract option chains for S&P 500 component companies and some SPDR ETFs. The transform/load script will just insert options expiring 2 weeks, 4 weeks, and 8 weeks from the current date and they will also just grab the +/- 0%, 2%, 4% and 8% strikes. This suits my purposes, but this filter can be removed if you want to insert everything.
