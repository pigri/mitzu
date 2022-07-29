import sys

import mitzu.webapp.persistence as P
import serverless.app.dash_app as A


def run():
    app = A.create_app(P.PathPersistencyProvider("serverless/app/demo/"))
    app.run_server(debug=True)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--profile":
        import cProfile

        cProfile.run("run()", sort="cumulative")
    else:
        run()
