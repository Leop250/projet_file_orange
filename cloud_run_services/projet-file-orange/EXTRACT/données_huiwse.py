import requests

def query_opendatasoft(domain: str,
                        dataset_id: str,
                        select: str = None,
                        where: str = None,
                        group_by: str = None,
                        order_by: str = None,
                        limit: int = 10,
                        offset: int = 0,
                        apikey: str = None) -> dict:
    """
    Interroge l’API OpenDataSoft / Explore v2.1.

    Args:
        domain: le domaine du portail (ex: "public.opendatasoft.com").
        dataset_id: l’identifiant du dataset.
        select, where, group_by, order_by: clauses ODSQL (optionnel).
        limit: nombre maximal de résultats.
        offset: position de départ.
        apikey: clé API (si nécessaire).
    Returns:
        Le JSON de réponse (dictionnaire)
    """
    base_url = f"https://{domain}/api/explore/v2.1/catalog/datasets/{dataset_id}/records"
    params = {
        "limit": limit,
        "offset": offset
    }
    if select:
        params["select"] = select
    if where:
        params["where"] = where
    if group_by:
        params["group_by"] = group_by
    if order_by:
        params["order_by"] = order_by

    headers = {"Authorization": f"Apikey {apikey}"} if apikey else {}

    resp = requests.get(base_url, params=params, headers=headers)
    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    # Exemple avec un dataset public
    domain = "public.opendatasoft.com"
    dataset_id = "san-francisco-calls-for-service"
    apikey = None  # Pas nécessaire pour un dataset public

    result = query_opendatasoft(
        domain=domain,
        dataset_id=dataset_id,
        limit=10,  # On récupère juste les 10 premiers enregistrements
        offset=0,
        apikey=apikey
    )

    print("Total count:", result.get("total_count"))
    print("\n10 premiers enregistrements :")
    for rec in result.get("results", []):
        print(rec["record"]["fields"])
