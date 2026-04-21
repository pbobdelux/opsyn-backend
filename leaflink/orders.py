@app.get("/leaflink/orders")
def get_orders():
    url = f"{LEAFLINK_BASE_URL}/orders"
    
    response = requests.get(
        url,
        headers={
            "Authorization": f"App {LEAFLINK_API_KEY}"
        },
        timeout=10
    )

    if response.status_code != 200:
        return {
            "success": False,
            "status": response.status_code,
            "error": response.text
        }

    return {
        "success": True,
        "orders": response.json()
    }