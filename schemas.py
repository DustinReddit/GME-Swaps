import pyarrow as pa


def make_fields_optional(schema):
    return pa.schema(
        [pa.field(field.name, field.type, nullable=True) for field in schema]
    )


PRE_2023 = make_fields_optional(
    pa.schema(
        [
            pa.field("Dissemination ID", pa.int64()),
            pa.field("Original Dissemination ID", pa.int64()),
            pa.field("Primary Asset Class", pa.string()),
            pa.field("Product ID", pa.string()),
            pa.field("Action", pa.string()),
            pa.field("Transaction Type", pa.string()),
            pa.field("Block Trade Election Indicator", pa.string()),
            pa.field("Cleared", pa.string()),
            pa.field("Clearing Exception or Exemption Indicator", pa.string()),
            pa.field("Day Count Convention", pa.string()),
            pa.field("Effective Date", pa.date32()),
            pa.field("Embedded Option Type", pa.string()),
            pa.field("Event Timestamp", pa.timestamp("s")),
            pa.field("Exchange Rate", pa.float64()),
            pa.field("Exchange Rate Basis", pa.float64()),
            pa.field("Execution Timestamp", pa.timestamp("s")),
            pa.field("Expiration Date", pa.date32()),
            pa.field("First Exercise Date", pa.date32()),
            pa.field("Fixed Rate 1", pa.float64()),
            pa.field("Fixed Rate 2", pa.float64()),
            pa.field("Fixed Recovery CDS Final Price", pa.float64()),
            pa.field("Floating Rate Reset Frequency Period 1", pa.string()),
            pa.field("Floating Rate Reset Frequency Period 2", pa.string()),
            pa.field("Floating Rate Reset Frequency Period Multiplier 1", pa.int64()),
            pa.field("Floating Rate Reset Frequency Period Multiplier 2", pa.int64()),
            pa.field("Leg 1 - Commodity Underlyer ID", pa.string()),
            pa.field("Leg 2 - Commodity Underlyer ID", pa.string()),
            pa.field("Leg 1 - Floating Rate Index", pa.string()),
            pa.field("Leg 2 - Floating Rate Index", pa.string()),
            pa.field("Non-Standardized Pricing Indicator", pa.string()),
            pa.field("Notional Amount 1", pa.string()),
            pa.field("Notional Amount 2", pa.string()),
            pa.field("Notional Currency 1", pa.string()),
            pa.field("Notional Currency 2", pa.string()),
            pa.field("Notional Quantity 1", pa.int64()),
            pa.field("Notional Quantity 2", pa.int64()),
            pa.field("Total Notional Quantity 1", pa.string()),
            pa.field("Total Notional Quantity 2", pa.string()),
            pa.field("Option Entitlement", pa.int64()),
            pa.field("Option Premium Amount", pa.string()),
            pa.field("Option Premium Currency", pa.string()),
            pa.field("Other Payment Amount", pa.string()),
            pa.field("Payment Frequency Period 1", pa.string()),
            pa.field("Payment Frequency Period 2", pa.string()),
            pa.field("Payment Frequency Period Multiplier 1", pa.int64()),
            pa.field("Payment Frequency Period Multiplier 2", pa.int64()),
            pa.field("Price 1", pa.string()),
            pa.field("Price 2", pa.string()),
            pa.field("Price Unit Of Measure 1", pa.string()),
            pa.field("Price Unit Of Measure 2", pa.string()),
            pa.field("Quantity Frequency", pa.string()),
            pa.field("Quantity Unit Of Measure", pa.string()),
            pa.field("Settlement Currency 1", pa.string()),
            pa.field("Settlement Currency 2", pa.string()),
            pa.field("Spread 1", pa.string()),
            pa.field("Spread 2", pa.string()),
            pa.field("Spread Currency 1", pa.string()),
            pa.field("Spread Currency 2", pa.string()),
            pa.field("Strike Price", pa.string()),
            pa.field("Strike Price Currency", pa.string()),
            pa.field("Underlying Asset ID", pa.string()),
            pa.field("Underlying Asset ID Type", pa.string()),
            pa.field("Underlying Asset Name", pa.string()),
            pa.field("Leg 1 - Commodity Instrument ID", pa.string()),
            pa.field("Leg 2 - Commodity Instrument ID", pa.string()),
            pa.field("Option Type", pa.string()),
            pa.field("Option Style", pa.string()),
            pa.field("Execution Venue Type", pa.string()),
            pa.field("Collateralization Type", pa.string()),
        ]
    )
)

PRE_PHASE_2 = make_fields_optional(
    pa.schema(
        [
            pa.field("Dissemination Identifier", pa.int64()),
            pa.field("Original Dissemination Identifier", pa.int64()),
            pa.field("Action type", pa.string()),
            pa.field("Event type", pa.string()),
            pa.field("Event timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Amendment indicator", pa.bool_()),
            pa.field("Asset Class", pa.string()),
            pa.field("Product name", pa.string()),
            pa.field("Cleared", pa.string()),
            pa.field("Mandatory clearing indicator", pa.string()),
            pa.field("Execution Timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Effective Date", pa.date32()),
            pa.field("Expiration Date", pa.date32()),
            pa.field("Maturity date of the underlier", pa.date32()),
            pa.field("Non-standardized term indicator", pa.string()),
            pa.field("Platform identifier", pa.string()),
            pa.field("Prime brokerage transaction indicator", pa.string()),
            pa.field("Block trade election indicator", pa.string()),
            pa.field(
                "Large notional off-facility swap election indicator", pa.string()
            ),
            pa.field("Notional amount-Leg 1", pa.string()),
            pa.field("Notional amount-Leg 2", pa.string()),
            pa.field("Notional currency-Leg 1", pa.string()),
            pa.field("Notional currency-Leg 2", pa.string()),
            pa.field("Notional quantity-Leg 1", pa.int64()),
            pa.field("Notional quantity-Leg 2", pa.int64()),
            pa.field("Total notional quantity-Leg 1", pa.int64()),
            pa.field("Total notional quantity-Leg 2", pa.int64()),
            pa.field("Quantity frequency multiplier-Leg 1", pa.int64()),
            pa.field("Quantity frequency multiplier-Leg 2", pa.int64()),
            pa.field("Quantity unit of measure-Leg 1", pa.string()),
            pa.field("Quantity unit of measure-Leg 2", pa.string()),
            pa.field("Quantity frequency-Leg 1", pa.int64()),
            pa.field("Quantity frequency-Leg 2", pa.int64()),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 1",
                pa.int64(),
            ),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 2",
                pa.int64(),
            ),
            pa.field("Effective date of the notional amount-Leg 1", pa.date32()),
            pa.field("Effective date of the notional amount-Leg 2", pa.date32()),
            pa.field("End date of the notional amount-Leg 1", pa.date32()),
            pa.field("End date of the notional amount-Leg 2", pa.date32()),
            pa.field("Call amount-Leg 1", pa.float64()),
            pa.field("Call amount-Leg 2", pa.float64()),
            pa.field("Call currency-Leg 1", pa.string()),
            pa.field("Call currency-Leg 2", pa.string()),
            pa.field("Put amount-Leg 1", pa.float64()),
            pa.field("Put amount-Leg 2", pa.float64()),
            pa.field("Put currency-Leg 1", pa.string()),
            pa.field("Put currency-Leg 2", pa.string()),
            pa.field("Exchange rate", pa.float64()),
            pa.field("Exchange rate basis", pa.float64()),
            pa.field("First exercise date", pa.date32()),
            pa.field("Fixed rate-Leg 1", pa.float64()),
            pa.field("Fixed rate-Leg 2", pa.float64()),
            pa.field("Option Premium Amount", pa.string()),
            pa.field("Option Premium Currency", pa.string()),
            pa.field("Price", pa.string()),
            pa.field("Price unit of measure", pa.string()),
            pa.field("Spread-Leg 1", pa.string()),
            pa.field("Spread-Leg 2", pa.string()),
            pa.field("Spread currency-Leg 1", pa.string()),
            pa.field("Spread currency-Leg 2", pa.string()),
            pa.field("Strike Price", pa.float64()),
            pa.field("Strike price currency/currency pair", pa.string()),
            pa.field("Post-priced swap indicator", pa.bool_()),
            pa.field("Price currency", pa.string()),
            pa.field("Price notation", pa.int64()),
            pa.field("Spread notation-Leg 1", pa.int64()),
            pa.field("Spread notation-Leg 2", pa.int64()),
            pa.field("Strike price notation", pa.string()),
            pa.field("Fixed rate day count convention-leg 1", pa.string()),
            pa.field("Fixed rate day count convention-leg 2", pa.string()),
            pa.field("Floating rate day count convention-leg 1", pa.string()),
            pa.field("Floating rate day count convention-leg 2", pa.string()),
            pa.field("Floating rate reset frequency period-leg 1", pa.string()),
            pa.field("Floating rate reset frequency period-leg 2", pa.string()),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 2", pa.int64()
            ),
            pa.field("Other payment amount", pa.float64()),
            pa.field("Fixed rate payment frequency period-Leg 1", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 1", pa.string()),
            pa.field("Fixed rate payment frequency period-Leg 2", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 2", pa.string()),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field("Other payment type", pa.string()),
            pa.field("Other payment currency", pa.string()),
            pa.field("Settlement currency-Leg 1", pa.string()),
            pa.field("Settlement currency-Leg 2", pa.string()),
            pa.field("Settlement location-Leg 1", pa.string()),
            pa.field("Settlement location-Leg 2", pa.string()),
            pa.field("Collateralisation category", pa.string()),
            pa.field("Custom basket indicator", pa.bool_()),
            pa.field("Index factor", pa.string()),
            pa.field("Underlier ID-Leg 1", pa.string()),
            pa.field("Underlier ID-Leg 2", pa.string()),
            pa.field("Underlier ID source-Leg 1", pa.string()),
            pa.field("Underlying Asset Name", pa.string()),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 1",
                pa.string(),
            ),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 2",
                pa.string(),
            ),
            pa.field("Embedded Option type", pa.string()),
            pa.field("Option Type", pa.string()),
            pa.field("Option Style", pa.string()),
            pa.field("Package indicator", pa.bool_()),
            pa.field("Package transaction price", pa.float64()),
            pa.field("Package transaction price currency", pa.string()),
            pa.field("Package transaction price notation", pa.string()),
            pa.field("Package transaction spread", pa.float64()),
            pa.field("Package transaction spread currency", pa.string()),
            pa.field("Package transaction spread notation", pa.string()),
            pa.field("Physical delivery location-Leg 1", pa.string()),
            pa.field("Delivery Type", pa.string()),
        ]
    )
)

PHASE_2 = make_fields_optional(
    pa.schema(
        [
            pa.field("Dissemination Identifier", pa.int64()),
            pa.field("Original Dissemination Identifier", pa.int64()),
            pa.field("Action type", pa.string()),
            pa.field("Event type", pa.string()),
            pa.field("Event timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Amendment indicator", pa.bool_()),
            pa.field("Asset Class", pa.string()),
            pa.field("Product name", pa.string()),
            pa.field("Cleared", pa.string()),
            pa.field("Mandatory clearing indicator", pa.string()),
            pa.field("Execution Timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Effective Date", pa.date32()),
            pa.field("Expiration Date", pa.date32()),
            pa.field("Maturity date of the underlier", pa.date32()),
            pa.field("Non-standardized term indicator", pa.string()),
            pa.field("Platform identifier", pa.string()),
            pa.field("Prime brokerage transaction indicator", pa.string()),
            pa.field("Block trade election indicator", pa.string()),
            pa.field(
                "Large notional off-facility swap election indicator", pa.string()
            ),
            pa.field("Notional amount-Leg 1", pa.string()),
            pa.field("Notional amount-Leg 2", pa.string()),
            pa.field("Notional currency-Leg 1", pa.string()),
            pa.field("Notional currency-Leg 2", pa.string()),
            pa.field("Notional quantity-Leg 1", pa.int64()),
            pa.field("Notional quantity-Leg 2", pa.int64()),
            pa.field("Total notional quantity-Leg 1", pa.string()),
            pa.field("Total notional quantity-Leg 2", pa.string()),
            pa.field("Quantity frequency multiplier-Leg 1", pa.int64()),
            pa.field("Quantity frequency multiplier-Leg 2", pa.int64()),
            pa.field("Quantity unit of measure-Leg 1", pa.string()),
            pa.field("Quantity unit of measure-Leg 2", pa.string()),
            pa.field("Quantity frequency-Leg 1", pa.int64()),
            pa.field("Quantity frequency-Leg 2", pa.int64()),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 1",
                pa.string(),
            ),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 2",
                pa.string(),
            ),
            pa.field("Effective date of the notional amount-Leg 1", pa.date32()),
            pa.field("Effective date of the notional amount-Leg 2", pa.date32()),
            pa.field("End date of the notional amount-Leg 1", pa.date32()),
            pa.field("End date of the notional amount-Leg 2", pa.date32()),
            pa.field("Call amount", pa.float64()),
            pa.field("Call currency", pa.string()),
            pa.field("Put amount", pa.float64()),
            pa.field("Put currency", pa.string()),
            pa.field("Exchange rate", pa.float64()),
            pa.field("Exchange rate basis", pa.float64()),
            pa.field("First exercise date", pa.date32()),
            pa.field("Fixed rate-Leg 1", pa.float64()),
            pa.field("Fixed rate-Leg 2", pa.float64()),
            pa.field("Option Premium Amount", pa.string()),
            pa.field("Option Premium Currency", pa.string()),
            pa.field("Price", pa.string()),
            pa.field("Price unit of measure", pa.string()),
            pa.field("Spread-Leg 1", pa.string()),
            pa.field("Spread-Leg 2", pa.string()),  # ??
            pa.field("Spread currency-Leg 1", pa.string()),
            pa.field("Spread currency-Leg 2", pa.string()),
            pa.field("Strike Price", pa.string()),
            pa.field("Strike price currency/currency pair", pa.string()),
            pa.field("Post-priced swap indicator", pa.bool_()),
            pa.field("Price currency", pa.string()),
            pa.field("Price notation", pa.int64()),
            pa.field("Spread notation-Leg 1", pa.int64()),
            pa.field("Spread notation-Leg 2", pa.int64()),
            pa.field("Strike price notation", pa.int64()),
            pa.field("Fixed rate  count convention-leg 1", pa.string()),
            pa.field("Fixed rate  count convention-leg 2", pa.string()),
            pa.field("Floating rate  count convention-leg 1", pa.string()),
            pa.field("Floating rate  count convention-leg 2", pa.string()),
            pa.field("Floating rate reset frequency period-leg 1", pa.string()),
            pa.field("Floating rate reset frequency period-leg 2", pa.string()),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 2", pa.int64()
            ),
            pa.field("Other payment amount", pa.string()),
            pa.field("Fixed rate payment frequency period-Leg 1", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 1", pa.string()),
            pa.field("Fixed rate payment frequency period-Leg 2", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 2", pa.string()),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field("Other payment type", pa.string()),
            pa.field("Other payment currency", pa.string()),
            pa.field("Settlement currency-Leg 1", pa.string()),
            pa.field("Settlement currency-Leg 2", pa.string()),
            pa.field("Settlement location", pa.string()),
            pa.field("Collateralisation category", pa.string()),
            pa.field("Custom basket indicator", pa.bool_()),
            pa.field("Index factor", pa.string()),
            pa.field("Underlier ID-Leg 1", pa.string()),
            pa.field("Underlier ID-Leg 2", pa.string()),
            pa.field("Underlier ID source-Leg 1", pa.string()),
            pa.field("Underlying Asset Name", pa.string()),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 1",
                pa.string(),
            ),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 2",
                pa.string(),
            ),
            pa.field("Embedded Option type", pa.string()),
            pa.field("Option Type", pa.string()),
            pa.field("Option Style", pa.string()),
            pa.field("Package indicator", pa.bool_()),
            pa.field("Package transaction price", pa.string()),
            pa.field("Package transaction price currency", pa.string()),
            pa.field("Package transaction price notation", pa.int64()),
            pa.field("Package transaction spread", pa.float64()),
            pa.field("Package transaction spread currency", pa.string()),
            pa.field("Package transaction spread notation", pa.string()),
            pa.field("Physical delivery location-Leg 1", pa.string()),
            pa.field("Delivery Type", pa.string()),
            pa.field("Unique Product Identifier", pa.string()),
            pa.field("UPI FISN", pa.string()),
            pa.field("UPI Underlier Name", pa.string()),
        ]
    )
)


CORRELATED_CUSTOM = make_fields_optional(
    pa.schema(
        [
            pa.field("Dissemination Identifier", pa.int64()),
            pa.field("Original Dissemination Identifier", pa.int64()),
            pa.field("Progenitor Dissemination Identifier", pa.int64()),
            pa.field("Action type", pa.string()),
            pa.field("Event type", pa.string()),
            pa.field("Event timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Amendment indicator", pa.bool_()),
            pa.field("Asset Class", pa.string()),
            pa.field("Product name", pa.string()),
            pa.field("Cleared", pa.string()),
            pa.field("Mandatory clearing indicator", pa.string()),
            pa.field("Execution Timestamp", pa.timestamp("s", tz="UTC")),
            pa.field("Effective Date", pa.date32()),
            pa.field("Expiration Date", pa.date32()),
            pa.field("Maturity date of the underlier", pa.date32()),
            pa.field("Non-standardized term indicator", pa.string()),
            pa.field("Platform identifier", pa.string()),
            pa.field("Prime brokerage transaction indicator", pa.string()),
            pa.field("Block trade election indicator", pa.string()),
            pa.field(
                "Large notional off-facility swap election indicator", pa.string()
            ),
            pa.field("Notional amount-Leg 1", pa.string()),
            pa.field("Notional amount-Leg 2", pa.string()),
            pa.field("Notional currency-Leg 1", pa.string()),
            pa.field("Notional currency-Leg 2", pa.string()),
            pa.field("Notional quantity-Leg 1", pa.int64()),
            pa.field("Notional quantity-Leg 2", pa.int64()),
            pa.field("Total notional quantity-Leg 1", pa.string()),
            pa.field("Total notional quantity-Leg 2", pa.string()),
            pa.field("Quantity frequency multiplier-Leg 1", pa.int64()),
            pa.field("Quantity frequency multiplier-Leg 2", pa.int64()),
            pa.field("Quantity unit of measure-Leg 1", pa.string()),
            pa.field("Quantity unit of measure-Leg 2", pa.string()),
            pa.field("Quantity frequency-Leg 1", pa.int64()),
            pa.field("Quantity frequency-Leg 2", pa.int64()),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 1",
                pa.string(),
            ),
            pa.field(
                "Notional amount in effect on associated effective date-Leg 2",
                pa.string(),
            ),
            pa.field("Effective date of the notional amount-Leg 1", pa.date32()),
            pa.field("Effective date of the notional amount-Leg 2", pa.date32()),
            pa.field("End date of the notional amount-Leg 1", pa.date32()),
            pa.field("End date of the notional amount-Leg 2", pa.date32()),
            pa.field("Call amount", pa.float64()),
            pa.field("Call currency", pa.string()),
            pa.field("Put amount", pa.float64()),
            pa.field("Put currency", pa.string()),
            pa.field("Exchange rate", pa.float64()),
            pa.field("Exchange rate basis", pa.float64()),
            pa.field("First exercise date", pa.date32()),
            pa.field("Fixed rate-Leg 1", pa.float64()),
            pa.field("Fixed rate-Leg 2", pa.float64()),
            pa.field("Option Premium Amount", pa.string()),
            pa.field("Option Premium Currency", pa.string()),
            pa.field("Price", pa.string()),
            pa.field("Price unit of measure", pa.string()),
            pa.field("Spread-Leg 1", pa.string()),
            pa.field("Spread-Leg 2", pa.string()),  # ??
            pa.field("Spread currency-Leg 1", pa.string()),
            pa.field("Spread currency-Leg 2", pa.string()),
            pa.field("Strike Price", pa.string()),
            pa.field("Strike price currency/currency pair", pa.string()),
            pa.field("Post-priced swap indicator", pa.bool_()),
            pa.field("Price currency", pa.string()),
            pa.field("Price notation", pa.int64()),
            pa.field("Spread notation-Leg 1", pa.int64()),
            pa.field("Spread notation-Leg 2", pa.int64()),
            pa.field("Strike price notation", pa.int64()),
            pa.field("Fixed rate  count convention-leg 1", pa.string()),
            pa.field("Fixed rate  count convention-leg 2", pa.string()),
            pa.field("Floating rate  count convention-leg 1", pa.string()),
            pa.field("Floating rate  count convention-leg 2", pa.string()),
            pa.field("Floating rate reset frequency period-leg 1", pa.string()),
            pa.field("Floating rate reset frequency period-leg 2", pa.string()),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate reset frequency period multiplier-leg 2", pa.int64()
            ),
            pa.field("Other payment amount", pa.string()),
            pa.field("Fixed rate payment frequency period-Leg 1", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 1", pa.string()),
            pa.field("Fixed rate payment frequency period-Leg 2", pa.string()),
            pa.field("Floating rate payment frequency period-Leg 2", pa.string()),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 1", pa.int64()
            ),
            pa.field(
                "Fixed rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field(
                "Floating rate payment frequency period multiplier-Leg 2", pa.int64()
            ),
            pa.field("Other payment type", pa.string()),
            pa.field("Other payment currency", pa.string()),
            pa.field("Settlement currency-Leg 1", pa.string()),
            pa.field("Settlement currency-Leg 2", pa.string()),
            pa.field("Settlement location", pa.string()),
            pa.field("Collateralisation category", pa.string()),
            pa.field("Custom basket indicator", pa.bool_()),
            pa.field("Index factor", pa.string()),
            pa.field("Underlier ID-Leg 1", pa.string()),
            pa.field("Underlier ID-Leg 2", pa.string()),
            pa.field("Underlier ID source-Leg 1", pa.string()),
            pa.field("Underlying Asset Name", pa.string()),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 1",
                pa.string(),
            ),
            pa.field(
                "Underlying asset subtype or underlying contract subtype-Leg 2",
                pa.string(),
            ),
            pa.field("Embedded Option type", pa.string()),
            pa.field("Option Type", pa.string()),
            pa.field("Option Style", pa.string()),
            pa.field("Package indicator", pa.bool_()),
            pa.field("Package transaction price", pa.string()),
            pa.field("Package transaction price currency", pa.string()),
            pa.field("Package transaction price notation", pa.int64()),
            pa.field("Package transaction spread", pa.float64()),
            pa.field("Package transaction spread currency", pa.string()),
            pa.field("Package transaction spread notation", pa.string()),
            pa.field("Physical delivery location-Leg 1", pa.string()),
            pa.field("Delivery Type", pa.string()),
            pa.field("Unique Product Identifier", pa.string()),
            pa.field("UPI FISN", pa.string()),
            pa.field("UPI Underlier Name", pa.string()),
        ]
    )
)


def identify_schema(column_names):
    if "Primary Asset Class" in column_names:
        return PRE_2023
    elif "Call amount-Leg 1" in column_names:
        return PRE_PHASE_2
    elif "Unique Product Identifier" in column_names:
        return PHASE_2

    return None


def map_columns(table):
    if "Primary Asset Class" in table.column_names:
        map = {
            name: value
            for name, value in PRE_2023_TO_PHASE_2.items()
            if value is not None
        }
        table = table.rename_columns(map)
    elif "Call amount-Leg 1" in table.column_names:
        map = {
            name: value
            for name, value in PRE_PHASE_2_TO_PHASE_2.items()
            if value is not None
        }
        table = table.rename_columns(map)


PRE_2023_TO_PHASE_2 = {
    "Dissemination ID": "Dissemination Identifier",
    "Original Dissemination ID": "Original Dissemination Identifier",
    "Action": "Action type",
    "Block Trade Election Indicator": "Block trade election indicator",
    "Clearing Exception or Exemption Indicator": None,  # Is this right?
    "Collateralization Type": "Collateralisation category",
    "Day Count Convention": None,  # ?
    "Embedded Option Type": "Embedded Option type",
    "Event Timestamp": "Event timestamp",
    "Exchange Rate": "Exchange rate",
    "Exchange Rate Basis": "Exchange rate basis",
    "Execution Venue Type": None,  # ?
    "First Exercise Date": "First exercise date",
    "Fixed Rate 1": "Fixed rate-Leg 1",
    "Fixed Rate 2": "Fixed rate-Leg 2",
    "Fixed Recovery CDS Final Price": None,  # ?
    "Floating Rate Reset Frequency Period 1": "Floating rate reset frequency period-leg 1",
    "Floating Rate Reset Frequency Period 2": "Floating rate reset frequency period-leg 2",
    "Floating Rate Reset Frequency Period Multiplier 1": "Floating rate reset frequency period multiplier-leg 1",
    "Floating Rate Reset Frequency Period Multiplier 2": "Floating rate reset frequency period multiplier-leg 2",
    "Leg 1 - Commodity Instrument ID": None,  # ?
    "Leg 2 - Commodity Instrument ID": None,  # ?
    "Leg 1 - Commodity Underlyer ID": "Underlier ID-Leg 1",
    "Leg 2 - Commodity Underlyer ID": "Underlier ID-Leg 2",
    "Leg 1 - Floating Rate Index": "Underlier ID source-Leg 1",
    "Leg 2 - Floating Rate Index": None,  # ?
    "Non-Standardized Pricing Indicator": "Non-standardized term indicator",
    "Notional Amount 1": "Notional amount-Leg 1",
    "Notional Amount 2": "Notional amount-Leg 2",
    "Notional Currency 1": "Notional currency-Leg 1",
    "Notional Currency 2": "Notional currency-Leg 2",
    "Notional Quantity 1": "Notional quantity-Leg 1",
    "Notional Quantity 2": "Notional quantity-Leg 2",
    "Option Entitlement": None,  # ?
    "Other Payment Amount": "Other payment amount",
    "Payment Frequency Period 1": "Fixed rate payment frequency period-Leg 1",
    "Payment Frequency Period 2": "Fixed rate payment frequency period-Leg 2",
    "Payment Frequency Period Multiplier 1": "Fixed rate payment frequency period multiplier-Leg 1",
    "Payment Frequency Period Multiplier 2": "Fixed rate payment frequency period multiplier-Leg 2",
    "Price 1": "Price",
    "Price 2": None,  # ?
    "Price Unit Of Measure 1": "Price unit of measure",
    "Price Unit Of Measure 2": None,  # ?
    "Primary Asset Class": "Asset Class",
    "Product ID": "Product name",
    "Quantity Frequency": None,  # ?
    "Quantity Unit Of Measure": None,  # ?
    "Settlement Currency 1": "Settlement currency-Leg 1",
    "Settlement Currency 2": "Settlement currency-Leg 2",
    "Spread 1": "Spread-Leg 1",
    "Spread 2": "Spread-Leg 2",
    "Spread Currency 1": "Spread currency-Leg 1",
    "Spread Currency 2": "Spread currency-Leg 2",
    "Strike Price Currency": "Strike price currency/currency pair",
    "Underlying Asset ID": "Underlier ID-Leg 1",
    "Underlying Asset ID Type": "Underlier ID source-Leg 1",
    "Total Notional Quantity 1": "Total notional quantity-Leg 1",
    "Total Notional Quantity 2": "Total notional quantity-Leg 2",
    "Transaction Type": "Event type",
}

PRE_PHASE_2_TO_PHASE_2 = {
    "Call amount-Leg 1": "Call amount",
    "Call amount-Leg 2": None,
    "Call currency-Leg 1": "Call currency",
    "Call currency-Leg 2": None,
    "Fixed rate day count convention-leg 1": "Fixed rate  count convention-leg 1",
    "Fixed rate day count convention-leg 2": "Fixed rate  count convention-leg 2",
    "Floating rate day count convention-leg 1": "Floating rate  count convention-leg 1",
    "Floating rate day count convention-leg 2": "Floating rate  count convention-leg 2",
    "Put amount-Leg 1": "Put amount",
    "Put amount-Leg 2": None,
    "Put currency-Leg 1": "Put currency",
    "Put currency-Leg 2": None,
    "Settlement location-Leg 1": "Settlement location",
    "Settlement location-Leg 2": None,
}

if __name__ == "__main__":
    pre_columns = set(PRE_2023.names)
    mapped_pre_columns = set(
        [PRE_2023_TO_PHASE_2.get(name, name) for name in pre_columns]
    )

    phase_columns = set(PHASE_2.names)
    missing_columns = mapped_pre_columns - phase_columns
    missing_columns = sorted([m for m in missing_columns if m is not None])

    if len(missing_columns) > 0:
        print("Missing columns from PRE_2023 to PHASE_2:")
        for column in missing_columns:
            print(f"    {column}")
    else:
        print("All columns from PRE_2023 to PHASE_2 are present")

    # Verify that the types are the same
    for pre_name, phase_name in PRE_2023_TO_PHASE_2.items():
        if phase_name is None:
            continue

        pre_field = PRE_2023.field(pre_name)
        phase_field = PHASE_2.field(phase_name)

        if pre_field.type != phase_field.type:
            print(f"Type mismatch for {pre_name} -> {phase_name}")
            print(f"    PRE: {pre_field.type}")
            print(f"    PHASE: {phase_field.type}")

    pre_columns = set(PRE_PHASE_2.names)
    mapped_pre_columns = set(
        [PRE_PHASE_2_TO_PHASE_2.get(name, name) for name in pre_columns]
    )

    missing_columns = mapped_pre_columns - phase_columns
    missing_columns = sorted([m for m in missing_columns if m is not None])

    if len(missing_columns) > 0:
        print("Missing columns from PRE_PHASE_2 to PHASE_2:")
        for column in missing_columns:
            print(f"    {column}")
    else:
        print("All columns from PRE_PHASE_2 to PHASE_2 are present")

    # Verify that the types are the same
    for pre_name, phase_name in PRE_PHASE_2_TO_PHASE_2.items():
        if phase_name is None:
            continue

        pre_field = PRE_PHASE_2.field(pre_name)
        phase_field = PHASE_2.field(phase_name)

        if pre_field.type != phase_field.type:
            print(f"Type mismatch for {pre_name} -> {phase_name}")
            print(f"    PRE: {pre_field.type}")
            print(f"    PHASE: {phase_field.type}")
