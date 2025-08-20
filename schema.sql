CREATE SCHEMA oic;

CREATE TYPE oic.call_put AS ENUM
   ('Call',
    'Put');
    
CREATE TABLE oic.option_chain
(
  act_symbol text NOT NULL,
  expiration date NOT NULL,
  strike numeric NOT NULL,
  call_put oic.call_put NOT NULL,
  date date NOT NULL,
  bid numeric,
  ask numeric,
  model_price numeric,
  vol numeric,
  delta numeric,
  gamma numeric,
  theta numeric,
  vega numeric,
  rho numeric,
  CONSTRAINT option_chain_pkey PRIMARY KEY (date, act_symbol, expiration, strike, call_put),
  CONSTRAINT option_chain_act_symbol_fkey FOREIGN KEY (act_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE oic.volatility_history
(
  act_symbol text NOT NULL,
  date date NOT NULL,
  hv_current numeric,
  hv_week_ago numeric,
  hv_month_ago numeric,
  hv_year_high numeric,
  hv_year_high_date date,
  hv_year_low numeric,
  hv_year_low_date date,
  iv_current numeric,
  iv_week_ago numeric,
  iv_month_ago numeric,
  iv_year_high numeric,
  iv_year_high_date date,
  iv_year_low numeric,
  iv_year_low_date date,
  CONSTRAINT volatility_history_pkey PRIMARY KEY (act_symbol, date),
  CONSTRAINT volatility_history_act_symbol_fkey FOREIGN KEY (act_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE oic.weekly
(
  act_symbol text NOT NULL,
  effective_date date NOT NULL,
  last_seen date NOT NULL,
  CONSTRAINT weekly_pkey PRIMARY KEY (act_symbol),
  CONSTRAINT weekly_act_symbol_fkey FOREIGN KEY (act_symbol) REFERENCES nasdaq.symbol(act_symbol)
);
