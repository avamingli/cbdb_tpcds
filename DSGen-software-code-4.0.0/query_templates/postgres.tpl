-- PostgreSQL dialect template for TPC-DS
-- PostgreSQL uses LIMIT n at end of query (same as Netezza)
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = "limit %d";
define _END = "";
