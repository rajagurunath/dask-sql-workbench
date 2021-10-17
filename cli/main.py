import asyncio
import logging
import os
from logging import FileHandler

from connections import get_dask_sql_context
from pyfiglet import Figlet
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.traceback import Traceback
from rich.tree import Tree
from textual import events
from textual.app import App
from textual.reactive import Reactive
from textual.scrollbar import ScrollBar
from textual.views import DockView
from textual.widget import Widget
from textual.widgets import Button, Footer, Header, Placeholder, ScrollView, TreeControl

# from .widgets.scroll_view import ScrollView


logger = logging.getLogger(__name__)


def make_button(text: str, style: str) -> Button:
    """Create a button with the given Figlet label."""
    return Button(text, style=style, name=text)


def df_to_table(df):
    df = df.reset_index().rename(columns={"index": ""})
    label = "table"
    total = df.shape[0]
    columns = df.columns
    table = Table(
        title=f"{label} ({total} rows)", show_lines=True, expand=True, min_width=280
    )
    # table.add_column("index")
    for col in columns:
        table.add_column(col)

    rows = df.values[:20]
    for row in rows:
        row = [str(item) for item in row]
        table.add_row(*list(row))

    return table


class SQL(Widget):
    def __init__(self, name: str = None) -> None:
        super().__init__(name=name)
        self.sql = ""

    sql = Reactive("")
    button = Reactive(False)

    def render(self) -> RenderableType:
        # font=Figlet(font="mini")

        sql = Syntax(
            self.sql, "SQL", theme="monokai", line_numbers=True, line_range=(1, 30)
        )
        # sql=Text(font.renderText(self.sql),style="bold")
        # sql=Markdown(self.sql,inline_code_theme="SQL",inline_code_lexer="SQL")
        button = make_button("Execute", "white on Red")
        return Panel(sql, style="bold ")
        # self.grid.place(sql=Panel(sql,style="bold"))
        # self.grid.place(execute = button)

    def __rich_repr__(self):
        yield "name", self.name
        yield "sql", self.sql

    def get_sql(self):
        return self.sql

    async def on_key(self, event: events.InputEvent) -> None:
        # handle backspace
        if event.key.lower() == "ctrl+h":
            self.sql = self.sql[:-1]
        # handle tab
        elif event.key.lower() == "ctrl+i":
            self.sql = self.sql + "\t"
        # handle enter
        elif event.key.lower() in ["shift+tab", "ctrl+c", "shift+right"]:
            pass
        elif event.key.lower() == "enter":
            self.sql = self.sql + "\n"
        else:
            self.sql += event.key


class MyApp(App):
    """Just a test app."""

    async def on_load(self, event: events.Load) -> None:
        await self.bind("ctrl+c", "quit", "Quit")
        # await self.bind("q", "quit", "Quit")
        await self.bind("shift+tab", "run", "Run SQL")
        await self.bind("shift+right", "show_sidebar", "Show sidebar")
        await self.bind("shift+left", "hide_sidebar", "Hide sidebar")

    show_bar: Reactive[bool] = Reactive(False)
    # sql:Reactive(str) = Reactive("")

    async def watch_show_bar(self, show_bar: bool) -> None:
        self.animator.animate(self.schemas, "layout_offset_x", 0 if show_bar else -40)

    async def action_show_sidebar(self) -> None:
        self.show_bar = True

    async def action_hide_sidebar(self) -> None:
        self.show_bar = False

    async def action_run(self, sql=None):
        sqlList = self.sql_box.get_sql().strip("#").split(";")
        try:
            for sql in sqlList:
                if sql:
                    df = self.sql_context.sql(sql)
            if df is not None:
                await self.table_box.update(df_to_table(df.compute()))
        except Exception as e:
            await self.table_box.update(Panel(f"Exception occured {e}"))

    async def on_mount(self, event: events.Mount) -> None:

        view = await self.push_view(DockView())

        self.sql_context = get_dask_sql_context()
        header = Header()
        footer = Footer()
        self.sql_box = SQL()
        self.ee = ScrollView(auto_width=True)
        self.table_box = ScrollView(auto_width=True)
        # self.bar = Placeholder(name="left")

        self.tables = TreeControl("Tables", {})
        self.models = TreeControl("Models", {})
        self.functions = TreeControl("Functions", {})
        self.schemas = TreeControl("Schemas", {})

        async def add_schemas():
            # tree = Tree("Dask-SQL",guide_style="bold bright_blue")
            for schema_name, container in self.sql_context.schema.items():
                await self.schemas.add(
                    self.schemas.root.id,
                    schema_name,
                    {"schema_name": schema_name, "is_schema": True},
                )

                # await self.schemas.add(1,"table1",{"schema_name":schema_name,"table":"table1"})

            await self.schemas.root.expand()

            # await self.tables.root.expand()

        await self.call_later(add_schemas)

        await view.dock(header, edge="top")
        await view.dock(footer, edge="bottom")
        await view.dock(self.schemas, edge="left", size=40, z=1)
        self.schemas.layout_offset_x = -40

        # upper_box = Syntax(self.sql, "SQL", theme="monokai", line_numbers=True,line_range=(1,30))
        sub_view = DockView()
        # self.ee.update()
        await sub_view.dock(self.sql_box, self.table_box, edge="top")
        await view.dock(sub_view, edge="left")

    async def handle_tree_click(self, message):
        schema_name = message.node.data["schema_name"]
        schema = self.sql_context.schema[schema_name]

        async def load_schema(node):

            tables = schema.tables

            for name, table in tables.items():
                await node.add(
                    name,
                    {
                        "schema_name": schema_name,
                        "table_name": name,
                        "is_schema": False,
                    },
                )
                node.loaded = True

        async def add_sql_table():
            table_name = message.node.data.get("table_name", None)
            is_schema = message.node.data["is_schema"]

            if not is_schema:
                table = schema.tables[table_name]
                await self.table_box.update(df_to_table(table.df.compute()))

            elif not message.node.loaded:
                await load_schema(message.node)
                await message.node.expand()
            else:
                await message.node.toggle()

            # if table_name:
            #     table = schema.tables[table_name]
            #     await self.table_box.update(df_to_table(table.df.compute()))
            # else:
            #     await self.table_box.update("No table Name was choosed")

        # async def add_content():
        #     schema_name = message.node.data['schema_name']
        #     table_name = message.node.data.get("table_name",None)
        #     # table = batch.as_table()
        #     table = schema.tables[table_name]
        #     await self.table_box.update(df_to_table(table.df.compute()))

        # await self.call_later(add_content)
        await self.call_later(add_sql_table)


MyApp.run(log="textual.log")
