from reactpy import component, html
from reactpy.backend.fastapi import configure
from fastapi import FastAPI
from pathlib import Path

## Load the stylesheet
stylesheet_css = (
    Path('../../site_libs/bootstrap/bootstrap-dark.min.css')
    .read_text()
)

@component
def RayCharlesPhoto():
    return html.img(
        {
            "src": "https://picsum.photos/id/274/500/300",
            "style": {"width": "30%"},
            "alt": "Ray Charles",
        }
    )

@component
def PuppyPhoto():
    return html.img(
        {
            "src": "https://picsum.photos/id/237/500/300",
            "style": {"width": "50%"},
            "alt": "Puppy",
        }
    )

@component
def Photo(alt_text, image_id):
    return html.img(
        {
            "src": f"https://picsum.photos/id/{image_id}/500/200",
            "style": {"width": "50%"},
            "alt": alt_text,
        }
    )

@component
def Gallery():
    return html.section(
        html.h1("Photo Gallery"),
        Photo("Landscape", image_id=830),
        Photo("City", image_id=274),
        Photo("Puppy", image_id=237),
    )

@component
def Root():
    return html.div(
        {
            'class_name': 'container',
        },
        ## Add the stylesheet
        html.style(stylesheet_css),
        ## Add the h1 element
        html.h1(
            {
                'class_name': 'title',
            },
            "Hello, world!"
        ),
        ## Add the paragraph element
        html.p(
            "This is a paragraph"
        ),
        ## Add the list element
        html.ul(
            html.li("Build a cool new app"),
            html.li("Share it with the world!"),
            html.li("..."),
        ),
        ## Add the image element
        Gallery(),
    )

app = FastAPI()
configure(app, Root)
