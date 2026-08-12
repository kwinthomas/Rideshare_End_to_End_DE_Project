from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from connection import send_rides, send_to_event_hub
from data import generate_uber_ride_confirmation

app = FastAPI(title="Uber ride producer")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def booking_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/book", response_class=HTMLResponse)
def book_ride(request: Request):
    ride = generate_uber_ride_confirmation()
    sent = send_to_event_hub(ride)
    return templates.TemplateResponse(
        "confirmation.html",
        {"request": request, "ride": ride, "sent": sent},
    )


@app.get("/book/bulk")
def book_bulk(n: int = 100):
    """Load generator. /book/bulk?n=500 pushes 500 events in one go, which is
    how you get enough volume to see the streaming layer do real work."""
    n = max(1, min(n, 5000))
    rides = [generate_uber_ride_confirmation() for _ in range(n)]
    sent = send_rides(rides)
    return JSONResponse({"requested": n, "sent": sent})


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
