import great_expectations as gx

_context, _datasource, _suite_name, _suite = None, None, None, None
def get_gx():
    global _context, _datasource, _suite_name, _suite
    if not _context: # initialize Context (creates /gx folder)
        _context = gx.get_context()
    if not _datasource: # add Spark Datasource
        _datasource = _context.data_sources.add_spark(name="my_spark_datasource")
    if not _suite_name:
        _suite_name = "trade_quality_suite"
    if not _suite: # create Expectation Suite
        _suite = _context.suites.add(gx.ExpectationSuite(name=_suite_name))
    return _context, _datasource, _suite_name, _suite
    
def setup_suite(suite):
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="price", min_value=0.00001, max_value=1000000))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="ticker", value_set=["BTCUSDT", "ETHUSDT"]))


_validation_def = None
def get_validation_def():
    g = get_gx()
    
    try:
        data_asset = g[1].get_asset("trades_asset")
    except:
        data_asset = g[1].add_dataframe_asset(name="trades_asset")

    try:
        batch_definition = data_asset.get_batch_definition("all_trades")
    except KeyError:
        batch_definition = data_asset.add_batch_definition_whole_dataframe("all_trades")

    suite = g[0].suites.get("trade_quality_suite")

    try:
        validation_def = g[0].validation_definitions.get("trade_audit")
    except:
        # only create if doesn't exist
        validation_def = g[0].validation_definitions.add(
            gx.ValidationDefinition(
                name="trade_audit",
                data=batch_definition,
                suite=suite
            )
        )
    
    return validation_def