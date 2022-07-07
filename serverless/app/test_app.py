import mitzu.webapp.persistence as P
import serverless.app.dash_app as A

if __name__ == "__main__":
    app = A.create_app(P.PathPersistencyProvider("serverless/app/demo/"))
    app.run_server(debug=True)
