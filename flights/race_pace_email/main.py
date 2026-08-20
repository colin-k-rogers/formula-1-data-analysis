"""MotherDuck Flight: email a Race Pace Summary for the most recent 2025 F1 race.

On-demand flight. Reproduces the "Race detail" view of the 2025 Relative Lap
Pace dive (fct_lap_pace) for the most recent Race session: race header,
driver-level avg lap time / cumulative gap-to-fastest summary table, and a
lap-by-lap gap-to-fastest chart for the field's top 5 drivers by pace.

Requires two MotherDuck Flight secrets attached to this Flight:
- `gmail_smtp` (params SMTP_USERNAME, SMTP_PASSWORD — a Gmail address and app
  password)
- `email_recipient` (param RECIPIENT_EMAIL — the address summaries are sent to)
"""
import io
import os
import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

FCT_LAP_PACE = '"f1"."marts"."fct_lap_pace"'
PALETTE = ["#0777b3", "#bd4e35", "#2d7a00", "#e18727", "#638CAD"]


def format_lap_time(seconds):
    mins = int(seconds // 60)
    secs = seconds - mins * 60
    return f"{mins}:{secs:06.3f}"


def build_html(meeting_name, circuit_name, race_date, summary_rows):
    rows_html = "".join(
        f"<tr>"
        f"<td style='padding:4px 8px;color:#6a6a6a'>{i + 1}</td>"
        f"<td style='padding:4px 8px;color:#231f20;font-weight:600'>{r['driver_acronym']}</td>"
        f"<td style='padding:4px 8px;color:#6a6a6a'>{r['team_name']}</td>"
        f"<td style='padding:4px 8px;color:#231f20;text-align:right'>{format_lap_time(r['avg_lap_time'])}</td>"
        f"<td style='padding:4px 8px;color:#231f20;text-align:right'>+{r['cumulative_gap']:.3f}s</td>"
        f"</tr>"
        for i, r in enumerate(summary_rows)
    )
    return f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:640px;margin:0 auto">
      <h2 style="color:#231f20;margin-bottom:0">{meeting_name}</h2>
      <p style="color:#6a6a6a;margin-top:4px">{circuit_name} &mdash; {race_date}</p>
      <img src="cid:pace_chart" style="width:100%;max-width:600px;margin:12px 0" />
      <h3 style="color:#231f20;margin-bottom:4px">Race pace summary</h3>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <thead>
          <tr style="border-bottom:1px solid #ddd;color:#6a6a6a;text-align:left">
            <th style="padding:4px 8px">#</th>
            <th style="padding:4px 8px">Driver</th>
            <th style="padding:4px 8px">Team</th>
            <th style="padding:4px 8px;text-align:right">Avg lap time</th>
            <th style="padding:4px 8px;text-align:right">Cumulative gap to fastest</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def build_chart(lap_rows, top_drivers):
    fig, ax = plt.subplots(figsize=(6.4, 3.2), dpi=100)
    by_driver = {}
    for r in lap_rows:
        by_driver.setdefault(r["driver_acronym"], []).append(
            (r["lap_number"], r["delta_to_fastest"])
        )
    for i, driver in enumerate(top_drivers):
        points = sorted(by_driver.get(driver, []))
        if not points:
            continue
        laps, deltas = zip(*points)
        ax.plot(laps, deltas, label=driver, color=PALETTE[i % len(PALETTE)], linewidth=2)
    ax.set_xlabel("Lap")
    ax.set_ylabel("Gap to fastest lap (s)")
    ax.legend(loc="upper left", fontsize=8, ncol=len(top_drivers))
    ax.grid(True, linestyle="--", alpha=0.3)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def send_email(subject, html, chart_png, recipient):
    smtp_user = os.environ["gmail_smtp_SMTP_USERNAME"]
    smtp_password = os.environ["gmail_smtp_SMTP_PASSWORD"]

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html"))

    image = MIMEImage(chart_png)
    image.add_header("Content-ID", "<pace_chart>")
    image.add_header("Content-Disposition", "inline", filename="pace_chart.png")
    msg.attach(image)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, [recipient], msg.as_string())


def main():
    recipient = os.environ["email_recipient_RECIPIENT_EMAIL"]
    con = duckdb.connect("md:")
    con.execute("SET TimeZone = 'UTC';")

    sessions = con.execute(
        f"""
        select distinct
            session_key,
            meeting_official_name,
            circuit_short_name,
            session_date
        from {FCT_LAP_PACE}
        order by session_date asc
        """
    ).fetchall()
    if not sessions:
        print("No race sessions found; nothing to send.")
        return

    session_key, meeting_name, circuit_name, session_date = sessions[-1]
    race_date = session_date.strftime("%Y-%m-%d")

    summary_rows = (
        con.execute(
            f"""
            select
                driver_acronym,
                team_name,
                avg(lap_duration) as avg_lap_time,
                sum(delta_to_fastest) as cumulative_gap
            from {FCT_LAP_PACE}
            where session_key = {session_key}
            group by 1, 2
            order by cumulative_gap asc
            """
        )
        .fetch_df()
        .to_dict("records")
    )

    top_drivers = [r["driver_acronym"] for r in summary_rows[:5]]

    lap_rows = (
        con.execute(
            f"""
            select lap_number, driver_acronym, delta_to_fastest
            from {FCT_LAP_PACE}
            where session_key = {session_key}
            """
        )
        .fetch_df()
        .to_dict("records")
    )

    chart_png = build_chart(lap_rows, top_drivers)
    html = build_html(meeting_name, circuit_name, race_date, summary_rows)
    subject = f"F1 Race Pace Summary: {meeting_name}"
    send_email(subject, html, chart_png, recipient)

    print(f"Sent race pace summary for {meeting_name} ({race_date}) to {recipient}")


if __name__ == "__main__":
    main()
