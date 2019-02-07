# oic-options-chains
These Racket programs will download the OIC Options Chains HTML files and insert the data into a PostgreSQL database. The intended usage is:

```
$ racket extract.rkt
$ racket transform-load.rkt
```

The provided schema.sql file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a 
/var/tmp/oic/options-chains folder. This process also assumes you have loaded your database with the NASDAQ symbol file information.
This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project. These programs currently (2019-02-06) 
will just extract option chains for S&P 500 component companies. I will probably update this at some point to get options info for 
popular ETFs as well. The transform/load script will just insert options expiring 2 weeks, 4 weeks, and 8 weeks from the current date 
and they will also just grab the +/- 0%, 2%, and 4% strikes. This suits my purposes, but this filter can be removed if you want to
insert everything.

Finally, there is one parameter required to extract data correctly from OIC: j-session-id. You can find this value by doing the following:

1. Go to https://oic.ivolatility.com/oic_adv_options.j in your web browser
2. Load a stock (DRE will work)
3. Observe the URL change to now include `;jsessionid=aAbB-cC1dD23`

This `jsessionid` value is what you will need to use to properly run the extract.
