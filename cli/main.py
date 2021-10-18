import asyncio
import logging
import os
import time

from connections import get_dask_sql_context
from rich import box
from rich.columns import Columns
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.traceback import Traceback
from textual import events
from textual.app import App
from textual.reactive import Reactive
from textual.views import DockView
from textual.widget import Widget
from textual.widgets import Button, Footer, Header, ScrollView, TreeControl

# from .widgets.scroll_view import ScrollView


logger = logging.getLogger(__name__)


def make_button(text: str, style: str) -> Button:
    """Create a button with the given Figlet label."""
    return Button(text, style=style, name=text)


def df_to_table(df, label="table", time=None):
    df = df.reset_index().rename(columns={"index": ""})
    # label = "table"
    total = df.shape[0]
    columns = df.columns
    table = Table(
        title=f"{label} ({total} rows | {len(columns)-1} columns)",
        caption=f"Query executed in {time} secs",
        show_lines=True,
        expand=True,
        min_width=280,
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
        # self.scroll = ScrollView()

    sql = Reactive("")
    # button = Reactive(False)
    has_focus: Reactive[bool] = Reactive(False)
    mouse_over: Reactive[bool] = Reactive(False)

    def render(self) -> RenderableType:
        # font=Figlet(font="mini")

        sql = Syntax(
            self.sql, "SQL", theme="monokai", line_numbers=True, line_range=(1, 30)
        )
        # sql=Text(font.renderText(self.sql),style="bold")
        # sql=Markdown(self.sql,inline_code_theme="SQL",inline_code_lexer="SQL")
        # button = make_button("Execute", "white on Red")
        return Panel(
            sql,
            style="bold ",
            title=self.__class__.__name__,
            border_style="green" if self.mouse_over else "blue",
            box=box.HEAVY if self.has_focus else box.ROUNDED,
        )
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
        # Avoid printing these keys
        elif event.key.lower() in [
            "shift+tab",
            "ctrl+c",
            "shift+right",
            "shift+left",
            "ctrl+q",
        ]:
            pass
        elif event.key.lower() == "enter":
            self.sql = self.sql + "\n"
        else:
            self.sql += event.key


class DaskSQLApp(App):
    """Just a test app."""

    async def on_load(self, event: events.Load) -> None:
        await self.bind("ctrl+c", "quit", "Quit")
        # await self.bind("q", "quit", "Quit")
        await self.bind("shift+tab", "run", "Run SQL")
        await self.bind("shift+right", "show_sidebar", "Show sidebar")
        await self.bind("shift+left", "hide_sidebar", "Hide sidebar")
        await self.bind("ctrl+q", "sql_history", "SQL History")

    show_bar: Reactive[bool] = Reactive(False)
    # sql:Reactive(str) = Reactive("")

    async def watch_show_bar(self, show_bar: bool) -> None:
        self.animator.animate(self.schemas, "layout_offset_x", 0 if show_bar else -40)

    async def action_sql_history(self) -> None:
        await self.table_box.update(
            Columns([Panel(Syntax(sql, "SQL", code_width=280)) for sql in self.history])
        )

    async def action_show_sidebar(self) -> None:
        self.show_bar = True

    async def action_hide_sidebar(self) -> None:
        self.show_bar = False

    async def action_run(self, sql=None):
        self.history.append(self.sql_box.get_sql().strip("#"))
        sqlList = self.sql_box.get_sql().strip("#").split(";")
        try:
            st = time.time()
            for sql in sqlList:
                if sql:
                    df = self.sql_context.sql(sql.strip("\n"))
                et1 = time.time()
            if df is not None:
                pdf = df.compute()
                et = time.time()
                elapsed_time = round(et - st, 3)
                await self.table_box.update(
                    df_to_table(pdf, label="SQLTable", time=elapsed_time)
                )
            else:
                elapsed_time = round(et1 - st, 3)
                await self.table_box.update(
                    Text(f"Query Executed in {elapsed_time} secs", style="center bold")
                )
        except Exception as e:
            # await self.table_box.update(Panel(f"Exception occured {e}"))
            traceback = Traceback(
                width=250,
                extra_lines=3,
                theme=None,
                word_wrap=False,
                show_locals=False,
            )
            await self.table_box.update(Panel(traceback))

    async def add_schemas(self):
        # tree = Tree("Dask-SQL",guide_style="bold bright_blue")
        for schema_name, container in self.sql_context.schema.items():
            if schema_name not in self._schemas_displayed:
                await self.schemas.add(
                    self.schemas.root.id,
                    schema_name,
                    {"schema_name": schema_name, "is_schema": True},
                )
                self._schemas_displayed.append(schema_name)
        await self.schemas.root.expand()

    async def on_mount(self, event: events.Mount) -> None:

        view = await self.push_view(DockView())

        self.sql_context = get_dask_sql_context()
        header = Header()
        footer = Footer()
        self.sql_box = SQL()
        self.ee = ScrollView(auto_width=True)
        self.table_box = ScrollView(auto_width=True, style="center bold")
        self._schemas_displayed = []
        # self.bar = Placeholder(name="left")

        self.tables = TreeControl("Tables", {})
        self.models = TreeControl("Models", {})
        self.functions = TreeControl("Functions", {})
        self.schemas = TreeControl(
            "Schemas", {"is_schema": True, "schema_name": "Schemas"}
        )
        self.history = []

        await view.dock(header, edge="top")
        await view.dock(footer, edge="bottom")
        await view.dock(self.schemas, edge="left", size=40, z=1)
        self.schemas.layout_offset_x = -40
        sub_view = DockView()
        await sub_view.dock(self.sql_box, self.table_box, edge="top")
        await view.dock(sub_view, edge="left")

    async def handle_tree_click(self, message):

        schema_name = message.node.data.get("schema_name", None)
        if schema_name == "Schemas":
            logger.error(message.node.data)
            await self.add_schemas()
            return
        elif schema_name:
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

                await self.table_box.update(
                    df_to_table(table.df.compute(), label=f"{schema_name}.{table_name}")
                )

            elif not message.node.loaded:
                await load_schema(message.node)
                await message.node.expand()
            else:
                await message.node.toggle()

        await self.call_later(add_sql_table)


DaskSQLApp.run(title="Dask-SQL-workbench", log="dask-sql-textual.log")
