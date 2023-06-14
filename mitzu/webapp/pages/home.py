from dash import register_page
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, html
import mitzu.webapp.configs as configs
import mitzu.webapp.navbar as NB
import mitzu.webapp.pages.paths as P
import mitzu.webapp.dependencies as DEPS
from mitzu.webapp.auth.decorator import restricted_layout, restricted
import dash_mantine_components as dmc

import mitzu.webapp.onboarding_flow as OF


register_page(
    __name__,
    path=P.HOME_PATH,
    title="Mitzu",
)

ONBOARDING_DISMISS_BUTTON = "onboarding_dismiss_button"
ONBOARDING_STEPPER_ROW = "onboarding_stepper_row"
SKIP_BUTTON = "onboarding_skip_button"


@restricted_layout
def layout(**query_params):
    return html.Div(
        [
            NB.create_mitzu_navbar("home-navbar"),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            html.Img(
                                src=configs.DASH_LOGO_PATH,
                                height="100px",
                                className="logo",
                            )
                        ],
                        justify="center",
                    ),
                    html.Hr(),
                ]
                + onboarding()
            ),
        ]
    )


def onboarding_step_description_row(content: str = ""):
    return dbc.Col(
        children=[
            html.Div(content, className="text-align-center lead"),
        ],
        align="center",
        sm="auto",
    )


def onboarding():
    depenednecies = DEPS.Dependencies.get()
    onboarding_service = depenednecies.onboarding_service

    onboarding_flow = OF.ConfigureMitzuOnboardingFlow()

    configure_flow_state = onboarding_service.get_current_state(
        onboarding_flow.flow_id()
    )

    if configure_flow_state.current_state == onboarding_flow.terminating_state():
        return []

    active_step_id = onboarding_flow._states.index(configure_flow_state.current_state)

    return [
        dbc.Row(
            [
                dbc.Card(
                    children=[
                        dbc.CardHeader(
                            [
                                html.Span(
                                    "Let's get started", className="lead fw-bold"
                                ),
                                dbc.Button(
                                    "Skip", size="sm", color="light", id=SKIP_BUTTON
                                ),
                            ],
                            className="d-flex justify-content-between",
                        ),
                        dbc.CardBody(
                            dmc.Stepper(
                                active=active_step_id,
                                color="#06000D",
                                orientation="horizontal",
                                children=[
                                    dmc.StepperStep(
                                        label="Connect your data warehouse",
                                        children=[
                                            dbc.Row(
                                                [
                                                    onboarding_step_description_row(
                                                        """Create a new project and connect your data warehouse to Mitzu.                                                     
                                                        If you don't have a data warehouse yet you can also explore our sample one.
                                                        """,
                                                    ),
                                                ],
                                                class_name="mt-3",
                                                justify="center",
                                            ),
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        children=[
                                                            dbc.Button(
                                                                children=[
                                                                    html.I(
                                                                        className="bi bi-plus-circle me-1"
                                                                    ),
                                                                    "Create new project",
                                                                ],
                                                                href=P.PROJECTS_CREATE_PATH,
                                                                class_name="mb-3 me-3",
                                                                size="sm",
                                                            ),
                                                            dbc.Button(
                                                                children=[
                                                                    html.I(
                                                                        className="bi bi-box-arrow-up-right me-1"
                                                                    ),
                                                                    "Explore sample project",
                                                                ],
                                                                href=P.PROJECTS_PATH,
                                                                class_name="mb-3",
                                                                size="sm",
                                                                color="secondary",
                                                            ),
                                                        ],
                                                        sm="auto",
                                                        align="center",
                                                    ),
                                                ],
                                                justify="center",
                                                class_name="mt-3",
                                            ),
                                        ],
                                    ),
                                    dmc.StepperStep(
                                        label="Explore data",
                                        children=[
                                            dbc.Row(
                                                [
                                                    onboarding_step_description_row(
                                                        "Mitzu is ready to help you discover your user events."
                                                        "Let's find the first insight!",
                                                    ),
                                                ],
                                                class_name="mt-3",
                                                justify="center",
                                            ),
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        dbc.Button(
                                                            children=[
                                                                html.I(
                                                                    className="bi bi-arrow-up-right-circle-fill me-1"
                                                                ),
                                                                "Explore your data",
                                                            ],
                                                            href=P.PROJECTS_PATH,
                                                            size="sm",
                                                        ),
                                                        sm="auto",
                                                        align="center",
                                                    ),
                                                ],
                                                justify="center",
                                                class_name="mt-3",
                                            ),
                                        ],
                                    ),
                                    dmc.StepperStep(
                                        label="Invite your team",
                                        children=[
                                            dbc.Row(
                                                [
                                                    onboarding_step_description_row(
                                                        "Inviting team members will allow you to share insights with each other.",
                                                    ),
                                                ],
                                                class_name="mt-3",
                                                justify="center",
                                            ),
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        dbc.Button(
                                                            children=[
                                                                html.I(
                                                                    className="bi bi-people-fill me-1"
                                                                ),
                                                                "Invite team members",
                                                            ],
                                                            href=P.USERS_PATH,
                                                            size="sm",
                                                        ),
                                                        sm="auto",
                                                        align="center",
                                                    ),
                                                ],
                                                justify="center",
                                                class_name="mt-3",
                                            ),
                                        ],
                                    ),
                                    dmc.StepperCompleted(
                                        children=[
                                            dbc.Row(
                                                [
                                                    onboarding_step_description_row(
                                                        "Congratulations, you are all set!"
                                                    ),
                                                ],
                                                class_name="mt-3",
                                                justify="center",
                                            ),
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        dbc.Button(
                                                            id=ONBOARDING_DISMISS_BUTTON,
                                                            children=[
                                                                html.I(
                                                                    className="bi bi-plus-circle me-1"
                                                                ),
                                                                "Close",
                                                            ],
                                                            class_name="mb-3",
                                                            size="sm",
                                                        ),
                                                        sm="auto",
                                                        align="center",
                                                    ),
                                                ],
                                                class_name="mt-3",
                                                justify="center",
                                            ),
                                        ]
                                    ),
                                ],
                            )
                        ),
                    ],
                ),
            ],
            justify="center",
            id=ONBOARDING_STEPPER_ROW,
        ),
    ]


@callback(
    inputs=[
        Input(ONBOARDING_DISMISS_BUTTON, "n_clicks"),
        Input(SKIP_BUTTON, "n_clicks"),
    ],
    output=Output(ONBOARDING_STEPPER_ROW, "hidden"),
    prevent_initial_call=True,
)
@restricted
def dismiss_onboarding(n_clicks, skip_n_clicks):
    onboarding_service = DEPS.Dependencies.get().onboarding_service
    onboarding_service.mark_state_complete(
        OF.ConfigureMitzuOnboardingFlow.flow_id(),
        OF.WAITING_FOR_DISMISS,
    )
    return True
