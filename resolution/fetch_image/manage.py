"""

The purpose of CLI in this project is to help with the local development.

In a production setting, we will be using a micro-server to trigger various
command.

"""

import click

from subcommands.translation import fetch_image

@click.group()
def cli():
    pass


cli.add_command(fetch_image)


if __name__ == "__main__":
    cli()
