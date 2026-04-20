import typer

app = typer.Typer(help="Finances reporting CLI")


@app.callback()
def _root() -> None:
    """Finances reporting CLI."""


if __name__ == "__main__":
    app()
