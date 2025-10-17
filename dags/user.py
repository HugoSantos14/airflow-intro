from airflow.sdk import asset, Asset, Context

def user_info(user: Asset, context: Context, info: str) -> dict[str]:
    user_data = context["ti"].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True,
    )
    return user_data["results"][0][info]

@asset(
    schedule="@daily",
    uri="https://randomuser.me/api/"
)
def user(self) -> dict[str]:
    import requests
    return requests.get(self.uri).json()

@asset(
    schedule=user   # Assim que o user for materializado, este asset também será
)
def user_location(user: Asset, context: Context) -> dict[str]:
    return user_info(user, context, "location")

@asset(
    schedule=user
)
def user_login(user: Asset, context: Context) -> dict[str]:
    return user_info(user, context, "login")