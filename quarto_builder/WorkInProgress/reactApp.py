from reactpy import component, html
from reactpy.backend.fastapi import configure
from fastapi import FastAPI

@component
def HelloWorld():
    return html.div(
        html.h1("Hello, world!"),
        html.p("This is a paragraph"),
        html.ul(
            html.li("Build a cool new app"),
            html.li("Share it with the world!"),
        ),
    )

app = FastAPI()
configure(app, HelloWorld)
