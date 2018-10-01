Big Data in R
================
Peter Collins
September 26, 2018

Introduction
------------

Big Data problems in R can be defined as cases where a data set is too large to fit into memory of the local machine. In the exploratory phase of project we usually solve this problem by working with a subset of the data and this is usually achieves good results. Once a promising model is found using the initial subset of data, we might want to re-fit the model using all of the available data to re-fit the model. There are several approaches to the Big Data problem in R and the choice depends on the resources available (be it budgetary or time).

In this notebook we consider an example case where we encounter a Big Data problem and we show how we might solve it.

Connecting to the Database
--------------------------

Our first task is to connect to a data store. There are several functions in `dplyr` that allow us to access data in various formats, all of the form `src_{*store type*}`. Below we will connect to a RedShift database hosted on AWS, which uses PostgreSQL.

``` r
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
# Create connection to the database
air <- src_postgres(
  dbname = 'airontime', 
  host = 'sol-eng.cjku7otn8uia.us-west-2.redshift.amazonaws.com', 
  port = '5439', 
  user = 'redshift_user', 
  password = 'ABCd4321')
```

The function `src_tbls()` lists the tables contained in the database referenced by the connection object.

``` r
# List table names
src_tbls(air)
```

    ## [1] "flights"    "carriers"   "planes"     "zffhrierqk" "mworvizmbx"
    ## [6] "upldulewco" "clclgqdtfr"

If we want to work with one of these tables in R, we make a reference to them with `tbl()` from `dplyr`.

``` r
# Create a table reference with tbl
flights <- tbl(air, "flights")
carriers <- tbl(air, "carriers")
```

When we call one of the connection objects in R, it retrieves the first 10 rows of the table.

``` r
print(flights)
```

    ## # Source:   table<flights> [?? x 29]
    ## # Database: postgres 8.0.2
    ## #   [redshift_user@sol-eng.cjku7otn8uia.us-west-2.redshift.amazonaws.com:5439/airontime]
    ##     year month dayofmonth dayofweek deptime crsdeptime arrtime crsarrtime
    ##    <int> <int>      <int>     <int>   <int>      <int>   <int>      <int>
    ##  1  2008     1          2         3    1619       1615    1804       1808
    ##  2  2007     9          2         7    1609       1615    1804       1800
    ##  3  2008     1          4         5    1631       1615    1807       1808
    ##  4  2007     9          4         2    1609       1615    1745       1800
    ##  5  2008     1          5         6    1613       1615    1800       1808
    ##  6  2007     9          5         3    1610       1615    1744       1800
    ##  7  2008     1          7         1    1607       1615    1756       1810
    ##  8  2007     9          7         5    1608       1615    1749       1800
    ##  9  2008     1          9         3    1611       1615    1800       1810
    ## 10  2007     9          9         7    1610       1615    1754       1800
    ## # ... with more rows, and 21 more variables: uniquecarrier <chr>,
    ## #   flightnum <int>, tailnum <chr>, actualelapsedtime <int>,
    ## #   crselapsedtime <int>, airtime <chr>, arrdelay <int>, depdelay <int>,
    ## #   origin <chr>, dest <chr>, distance <int>, taxiin <chr>, taxiout <chr>,
    ## #   cancelled <int>, cancellationcode <chr>, diverted <int>,
    ## #   carrierdelay <chr>, weatherdelay <chr>, nasdelay <chr>,
    ## #   securitydelay <chr>, lateaircraftdelay <chr>

Note that there are many millions of rows in this database, more than can easily be held in memory:

``` r
# Count the rows in flights table
print(flights %>% count())
```

    ## # Source:   lazy query [?? x 1]
    ## # Database: postgres 8.0.2
    ## #   [redshift_user@sol-eng.cjku7otn8uia.us-west-2.redshift.amazonaws.com:5439/airontime]
    ##           n
    ##       <dbl>
    ## 1 123534969

Manipulating Data
-----------------

As seen in the previous code snippet, we can manipulate the tables in the database using regular R code. Below we perform some pre-processing on the `flights` table using `dlpyr` verbs:

``` r
# Manipulate the reference as if it were the actual table
clean <- flights %>%
  filter(!is.na(arrdelay), !is.na(depdelay)) %>%
  filter(depdelay > 15, depdelay < 240) %>%
  filter(year >= 2002 & year <= 2007) %>%
  select(year, arrdelay, depdelay, distance, uniquecarrier)
```

When this code is run it executes quite fast. This is because `dplyr` uses lazy-evaluation, meaning that the query is only evaluated as needed. If we ask to see `clean`, `dplyr` will show us the SQL query that will be executed when references to `clean` are made.

``` r
# Show the SQL query stored in `clean`
show_query(clean)
```

    ## <SQL>
    ## SELECT "year", "arrdelay", "depdelay", "distance", "uniquecarrier"
    ## FROM (SELECT *
    ## FROM (SELECT *
    ## FROM "flights"
    ## WHERE ((NOT((("arrdelay") IS NULL))) AND (NOT((("depdelay") IS NULL))))) "pqydmkcmur"
    ## WHERE (("depdelay" > 15.0) AND ("depdelay" < 240.0))) "xhnudvmrsy"
    ## WHERE ("year" >= 2002.0 AND "year" <= 2007.0)

As we have seen, `dplyr` will return only the first 10 rows of the table when we call the reference. If we would like to retrieve all of the data into R, we simply add `colect()` to the end of our chain of manipulations. A similar function is `collapse()`, which forces `dplyr` to run all code up to the `collapse()` function in the chain of manipulations. This can be useful if we want a new table to work from before any more filtering is performed. Below we demonstrate how these two functions can be used in a sampling example:

``` r
# Extract random 1% sample of training data
random <- clean %>%
  mutate(x = random()) %>%
  collapse() %>%
  filter(x <= 0.01) %>%
  select(-x) %>%
  collect()
```

Let's walk through this line-by-line. First we use `mutate()` to create a new variable `x` which is a random number between zero and one. Then we call `collapse()` to execute this query in the database, yielding a table with the new variable 'x'. Next we choose all rows with `x` less than or equal to 0.01 and then we drop the `x` variable with the `select()` statement. Finally, we bring the table into R using `collect()`. Now we have a tibble `random` stored in the local machine's memory:

``` r
print(random)
```

    ## # A tibble: 65,474 x 5
    ##     year arrdelay depdelay distance uniquecarrier
    ##  * <int>    <int>    <int>    <int> <chr>        
    ##  1  2004       64       58      654 DH           
    ##  2  2003       60       54      160 DH           
    ##  3  2004        9       16      160 DH           
    ##  4  2007       30       28      692 EV           
    ##  5  2003       19       22      692 EV           
    ##  6  2006       30       40      692 EV           
    ##  7  2004      188      165      692 EV           
    ##  8  2005       44       56      503 EV           
    ##  9  2005        8       20      503 EV           
    ## 10  2007       21       22      692 EV           
    ## # ... with 65,464 more rows

Fitting a Model
---------------

Now that we have a sample of data, we can proceed to analyse the data using R. For this exercises we will fit a linear model predicting a variable *g**a**i**n*, where
*g**a**i**n* = *depdelay * − * arrdelay*.
 We choose a model with predictors *d**e**p**d**e**l**a**y*, *d**i**s**t**a**n**c**e*, and *u**n**i**q**u**e**c**a**r**r**i**e**r*. To start, add the new variable to the table `random`:

``` r
# Create new variable `gain`
random$gain <- random$depdelay - random$arrdelay
```

Fit a linear regression model with `lm()`:

``` r
# Build model
mod <- lm(gain ~ depdelay + distance + factor(uniquecarrier), data = random)
```

In order to make predictions from this model on data in the database, a dataframe containing the model coefficients is made.

``` r
# Make coefficients lookup table
coefs <- dummy.coef(mod)
coefs_table <- data.frame(
  uniquecarrier = names(coefs$`factor(uniquecarrier)`),
  carrier_score = coefs$`factor(uniquecarrier)`,
  int_score = coefs$`(Intercept)`,
  dist_score = coefs$distance,
  delay_score = coefs$depdelay,
  row.names = NULL, 
  stringsAsFactors = FALSE
)
print(coefs_table)
```

    ##    uniquecarrier carrier_score  int_score  dist_score delay_score
    ## 1             9E     0.0000000 -0.3185058 0.003327575 -0.01160389
    ## 2             AA    -3.1659005 -0.3185058 0.003327575 -0.01160389
    ## 3             AQ     0.9321424 -0.3185058 0.003327575 -0.01160389
    ## 4             AS    -0.1198957 -0.3185058 0.003327575 -0.01160389
    ## 5             B6    -4.0806158 -0.3185058 0.003327575 -0.01160389
    ## 6             CO    -3.4244714 -0.3185058 0.003327575 -0.01160389
    ## 7             DH     1.5651681 -0.3185058 0.003327575 -0.01160389
    ## 8             DL    -4.0465044 -0.3185058 0.003327575 -0.01160389
    ## 9             EV     0.5754155 -0.3185058 0.003327575 -0.01160389
    ## 10            F9    -0.9408556 -0.3185058 0.003327575 -0.01160389
    ## 11            FL    -2.4549772 -0.3185058 0.003327575 -0.01160389
    ## 12            HA     0.8588875 -0.3185058 0.003327575 -0.01160389
    ## 13            HP     0.1943524 -0.3185058 0.003327575 -0.01160389
    ## 14            MQ    -2.9741730 -0.3185058 0.003327575 -0.01160389
    ## 15            NW    -3.1157677 -0.3185058 0.003327575 -0.01160389
    ## 16            OH    -0.4225034 -0.3185058 0.003327575 -0.01160389
    ## 17            OO    -0.3259470 -0.3185058 0.003327575 -0.01160389
    ## 18            TZ    -1.9133276 -0.3185058 0.003327575 -0.01160389
    ## 19            UA    -1.7054101 -0.3185058 0.003327575 -0.01160389
    ## 20            US    -1.2225751 -0.3185058 0.003327575 -0.01160389
    ## 21            WN     2.9729695 -0.3185058 0.003327575 -0.01160389
    ## 22            XE    -3.4014805 -0.3185058 0.003327575 -0.01160389
    ## 23            YV     2.1512287 -0.3185058 0.003327575 -0.01160389

Using the model coefficients, we score the test data in the database. Note that the training set and test set are non-overlapping.

``` r
# Score test data
score <- flights %>%
  filter(year == 2008) %>%
  filter(!is.na(arrdelay) & !is.na(depdelay) & !is.na(distance)) %>%
  filter(depdelay > 15 & depdelay < 240) %>%
  filter(arrdelay > -60 & arrdelay < 360) %>%
  select(arrdelay, depdelay, distance, uniquecarrier) %>%
  left_join(carriers, by = c('uniquecarrier' = 'code')) %>%
  left_join(coefs_table, copy = TRUE) %>%
  mutate(gain = depdelay - arrdelay) %>%
  mutate(pred = int_score + carrier_score + dist_score * distance + delay_score * depdelay) %>%
  group_by(description) %>%
  summarize(gain = mean(1.0 * gain), pred = mean(pred))
```

    ## Joining, by = "uniquecarrier"

``` r
scores <- collect(score)
```

    ## Warning: Missing values are always removed in SQL.
    ## Use `AVG(x, na.rm = TRUE)` to silence this warning

    ## Warning: Missing values are always removed in SQL.
    ## Use `AVG(x, na.rm = TRUE)` to silence this warning

``` r
print(scores)
```

    ## # A tibble: 20 x 3
    ##    description                                                 gain   pred
    ##  * <chr>                                                      <dbl>  <dbl>
    ##  1 Pinnacle Airlines Inc.                                     0.400  0.537
    ##  2 Comair Inc.                                               -2.20   0.288
    ##  3 Skywest Airlines Inc.                                     -1.20   0.121
    ##  4 US Airways Inc. (Merged with America West 9/05. Reportin…  2.40   1.01 
    ##  5 Expressjet Airlines Inc.                                  -0.300 -2.47 
    ##  6 American Eagle Airlines Inc.                              -2.50  -2.48 
    ##  7 Frontier Airlines Inc.                                    -0.300  1.27 
    ##  8 United Air Lines Inc.                                      2.00   0.743
    ##  9 Alaska Airlines Inc.                                       1.40   2.04 
    ## 10 Aloha Airlines Inc.                                        5.50   3.17 
    ## 11 Atlantic Southeast Airlines                                0.500  1.10 
    ## 12 Mesa Airlines Inc.                                         0.200  2.44 
    ## 13 American Airlines Inc.                                    -0.600 -0.567
    ## 14 Continental Air Lines Inc.                                 2.30  -0.397
    ## 15 Delta Air Lines Inc.                                      -1.40  -1.80 
    ## 16 Northwest Airlines Inc.                                   -2.80  -1.30 
    ## 17 Southwest Airlines Co.                                     4.10   4.24 
    ## 18 JetBlue Airways                                           -0.900 -1.51 
    ## 19 AirTran Airways Corporation                               -1.90  -0.988
    ## 20 Hawaiian Airlines Inc.                                    -0.900  3.21

What we have done here is computed the predicted scores entirely inside the database; the test data is never loaded into memory. A side-effect of this process is that we had to compute the scores manually with a `mutate()` verb. Some database software comes with basic statistical packages that allow for prediction without needing to specify the equation explicitly (c.f spark\_ml).

Closing Notes
-------------

Here we have demonstrated a typical sampling method for dealing with Big Data problems, however we have not looked at cases where we want to fit a model to all of the data in the database. Base R cannot solve these types of problems and packages with distributed computation must instead be used (e.g. `sparklyr`). This may seem like a severe limitation of R, but one should not underestimate the efficacy of sampling in statistical analysis. Sampling may yield a model that is "good enough" for our project or serve as a useful starting point of a larger project. Before spending time devising distributed learning solutions to a problem, start with a model derived from a sub-sample of the total data.
