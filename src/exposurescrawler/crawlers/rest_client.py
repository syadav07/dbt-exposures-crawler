import tableauserverclient as TSC
from functools import lru_cache


class TableauRestClient:
    """
    Thin wrapper around the official Tableau Server client.
    """

    def __init__(self, url: str, tableau_token: str, ):
        self.tableau_auth = TSC.PersonalAccessTokenAuth(token_name='crawler', personal_access_token=tableau_token, site_id='loom')
        self.server = TSC.Server(url, use_server_version=True)
        self.tableau_client = server.auth.sign_in(tableau_auth)

    @lru_cache(maxsize=None)
    def retrieve_workbook(self, workbook_id: str):
        with self.server.auth.sign_in(self.tableau_auth):
            workbook = self.server.workbooks.get_by_id(workbook_id)

        return workbook

    @lru_cache(maxsize=None)
    def retrieve_user(self, user_id: str):
        with self.server.auth.sign_in(self.tableau_auth):
            user = self.server.users.get_by_id(user_id)

        return user

    def run_metadata_api(self, query: str):
        with self.server.auth.sign_in(self.tableau_auth):
            response = self.server.metadata.query(query)

        return response['data']