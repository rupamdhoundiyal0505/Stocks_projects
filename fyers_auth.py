import webbrowser
from fyers_apiv3 import fyersModel

CLIENT_ID = "KUC4376MFF-100"
SECRET_KEY = "DUH08LDA5T"
REDIRECT_URI = "http://127.0.0.1:5000/"


RESPONSE_TYPE = "code"
GRANT_TYPE = "authorization_code"
STATE = "backtest_session"
ACCESS_TOKEN_FILE="access_token.txt"


def gen_auth_code():
    session = fyersModel.SessionModel(
    client_id=CLIENT_ID, 
    secret_key=SECRET_KEY, 
    redirect_uri=REDIRECT_URI, 
    response_type=RESPONSE_TYPE, 
    grant_type=GRANT_TYPE
    )
    auth_url = session.generate_authcode()
    print("Opening the following URL in your browser:")
    print(f"URL: {auth_url}")
    webbrowser.open(auth_url)
    return session

def get_access_token(session):
    auth_code = input("Enter the auth code: ").strip()
    session.set_token(auth_code)
    response = session.generate_token()

    if response.get("s") != "ok":
        raise RuntimeError(f"Failed to generate token: {response.get('err_r')}")

    access_token = response["access_token"]
    with open(ACCESS_TOKEN_FILE, "w") as f:
        f.write(access_token)
    
    return access_token


if __name__ == "__main__":
    session = gen_auth_code()
    get_access_token(session)
  