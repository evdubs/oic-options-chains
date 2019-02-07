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
  vol numeric,
  delta numeric,
  gamma numeric,
  theta numeric,
  vega numeric,
  rho numeric,
  CONSTRAINT option_chain_pkey PRIMARY KEY (act_symbol, expiration, strike, call_put, date),
  CONSTRAINT option_chain_act_symbol_fkey FOREIGN KEY (act_symbol)
      REFERENCES nasdaq.symbol (act_symbol) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
);
