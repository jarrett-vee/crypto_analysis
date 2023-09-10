import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash import dash_table
import pandas as pd
from backtesting import backtest_strategy
from sqlalchemy import create_engine
from config import DATABASE_URL, symbols

engine = create_engine(DATABASE_URL)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

options = [{"label": symbol, "value": symbol} for symbol in symbols]

table_style = {
    "backgroundColor": "#343a40",
    "color": "#FFF",
    "border": "1px solid #454d55",
}

app.layout = html.Div(
    [
        html.H1("Crypto Backtest Dashboard", style={"color": "#FFF"}),
        dcc.Dropdown(
            id="symbol-dropdown",
            options=options,
            value=symbols[0],
            style={"color": "#000"},
        ),
        html.Div(id="output-backtest-result", style={"color": "#FFF"}),
        html.H2("Buy Signals", style={"color": "#FFF"}),
        dash_table.DataTable(
            id="table-buy-signals",
            style_table=table_style,
            style_header={
                "backgroundColor": "#454d55",
                "fontWeight": "bold",
                "color": "#FFF",
            },
            style_cell={
                "backgroundColor": "#343a40",
                "color": "#FFF",
                "border": "1px solid #454d55",
            },
        ),
        html.H2("Sell Signals", style={"color": "#FFF"}),
        dash_table.DataTable(
            id="table-sell-signals",
            style_table=table_style,
            style_header={
                "backgroundColor": "#454d55",
                "fontWeight": "bold",
                "color": "#FFF",
            },
            style_cell={
                "backgroundColor": "#343a40",
                "color": "#FFF",
                "border": "1px solid #454d55",
            },
        ),
    ],
    style={"backgroundColor": "#343a40", "padding": "10px"},
)


@app.callback(
    [
        Output("output-backtest-result", "children"),
        Output("table-buy-signals", "data"),
        Output("table-sell-signals", "data"),
    ],
    [Input("symbol-dropdown", "value")],
)
def update_results(symbol):
    initial_capital = 50000
    investment_fraction = 0.4
    final_value = backtest_strategy(symbol, initial_capital, investment_fraction)
    result_text = f"Backtest for {symbol}: Initial Capital: {initial_capital}, Final Portfolio Value: {final_value:.2f}"

    buy_signals = pd.read_sql(
        f"SELECT * FROM buy_signals WHERE symbol = '{symbol}'", engine
    )
    sell_signals = pd.read_sql(
        f"SELECT * FROM sell_signals WHERE symbol = '{symbol}'", engine
    )

    return result_text, buy_signals.to_dict("records"), sell_signals.to_dict("records")


if __name__ == "__main__":
    app.run_server(debug=True)
