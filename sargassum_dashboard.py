#!/usr/bin/env python3
"""
sargassum_dashboard.py
======================
Dashboard Streamlit pour la surveillance des sargasses.
Visualise les données collectées par sargassum_collector.py.

Lancement : streamlit run sargassum_dashboard.py
"""

import html
import json
import sqlite3

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from folium import CircleMarker, DivIcon, FeatureGroup, LayerControl, Map, Marker, Popup
from streamlit_folium import st_folium

# ── Configuration page ────────────────────────────────────────────────────────

st.set_page_config(
    layout="wide",
    page_title="Sargassum Dashboard",
    page_icon="🌊",
)

# ── Constantes ────────────────────────────────────────────────────────────────

DAY_COLORS = {
    0: "#00c800",  # vert
    1: "#64c800",
    2: "#c8c800",  # jaune
    3: "#c89600",
    4: "#c86400",
    5: "#c80000",  # rouge
}

RISK_COLORS = {
    "none":   "#00c800",  # vert
    "low":    "#c8c800",  # jaune
    "medium": "#c86400",  # orange
    "high":   "#c80000",  # rouge
}

RISK_NUM = {"none": 0, "low": 1, "medium": 2, "high": 3}


# ── Helpers DB ────────────────────────────────────────────────────────────────

def get_connection(db_path: str) -> sqlite3.Connection | None:
    """Ouvre la connexion SQLite ou retourne None si le fichier est inaccessible."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def get_last_collected(db_path: str) -> str:
    """Retourne l'horodatage de la dernière collecte toutes tables confondues."""
    conn = get_connection(db_path)
    if conn is None:
        return "—"
    tables = [
        ("noaa_sir_reports",   "collected_at"),
        ("noaa_afai",          "collected_at"),
        ("copernicus_currents", "collected_at"),
        ("aviso_geostrophic",  "collected_at"),
        ("drift_predictions",  "simulated_at"),
        ("foresea_forecasts",  "collected_at"),
        ("sargassum_monitoring", "collected_at"),
    ]
    last = None
    try:
        for table, col in tables:
            try:
                row = conn.execute(
                    f"SELECT {col} FROM {table} ORDER BY {col} DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    ts = row[0]
                    if last is None or ts > last:
                        last = ts
            except sqlite3.OperationalError:
                pass
    finally:
        conn.close()
    return last or "—"



# ── Configuration carte par île ───────────────────────────────────────────────

ISLAND_MAP_CONFIG = {
    "Saint-Barth":   {"center": [17.897, -62.833], "zoom": 11},
    "Martinique":    {"center": [14.607, -61.009], "zoom": 10},
    "Guadeloupe":    {"center": [16.249, -61.534], "zoom": 10},
    "Marie-Galante": {"center": [15.927, -61.273], "zoom": 11},
}

# ── Loaders SQLite (cached) ───────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=300)
def load_noaa_sir(db_path: str) -> pd.DataFrame:
    """Charge les rapports NOAA SIR, triés du plus récent au plus ancien."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM noaa_sir_reports ORDER BY report_date DESC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_afai(db_path: str) -> pd.DataFrame:
    """Charge les données AFAI triées par date, avec conversion en Timestamp."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM noaa_afai ORDER BY data_date ASC",
            conn,
        )
        conn.close()
        if not df.empty and "data_date" in df.columns:
            df["data_date"] = pd.to_datetime(df["data_date"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_copernicus(db_path: str) -> pd.DataFrame:
    """Charge les courants Copernicus triés par date."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM copernicus_currents ORDER BY data_date ASC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_aviso(db_path: str) -> pd.DataFrame:
    """Charge les courants AVISO géostrophiques triés par date."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM aviso_geostrophic ORDER BY data_date ASC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_drift(db_path: str) -> pd.DataFrame:
    """
    Charge les prédictions de dérive de la simulation la plus récente,
    triées par day_offset.
    positions_json contient [[lon, lat], ...] — Folium attend [lat, lon].
    """
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        # Récupérer le simulated_at le plus récent
        row = conn.execute(
            "SELECT MAX(simulated_at) AS max_sim FROM drift_predictions"
        ).fetchone()
        if not row or not row["max_sim"]:
            conn.close()
            return pd.DataFrame()
        max_sim = row["max_sim"]
        df = pd.read_sql_query(
            """SELECT * FROM drift_predictions
               WHERE simulated_at = ?
               ORDER BY day_offset ASC""",
            conn,
            params=(max_sim,),
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_foresea(db_path: str) -> pd.DataFrame:
    """Charge la dernière entrée FORESEA CNRS."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM foresea_forecasts ORDER BY collected_at DESC LIMIT 1",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_sargassum_monitoring(db_path: str) -> pd.DataFrame:
    """
    Charge les articles Sargassum Monitoring.
    Fusionne le post principal avec les extra_posts JSON pour obtenir
    un DataFrame plat de tous les articles.
    """
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            "SELECT * FROM sargassum_monitoring ORDER BY collected_at DESC LIMIT 1",
            conn,
        )
        conn.close()
        if df.empty:
            return pd.DataFrame()

        row = df.iloc[0]
        rows = [{
            "date":    row.get("post_date", ""),
            "title":   row.get("post_title", ""),
            "excerpt": row.get("post_excerpt", ""),
            "url":     row.get("post_url", ""),
        }]

        extra_raw = row.get("extra_posts", "[]") or "[]"
        try:
            extra = json.loads(extra_raw)
            for p in extra:
                rows.append({
                    "date":    p.get("date", ""),
                    "title":   p.get("title", ""),
                    "excerpt": p.get("excerpt", ""),
                    "url":     p.get("url", ""),
                })
        except (json.JSONDecodeError, TypeError):
            pass

        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_beach_scores(db_path: str) -> pd.DataFrame:
    """Charge les derniers scores de risque par plage depuis beach_risk_scores."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        row = conn.execute(
            "SELECT MAX(computed_at) AS last FROM beach_risk_scores"
        ).fetchone()
        if not row or not row[0]:
            conn.close()
            return pd.DataFrame()
        computed_at = row[0]
        df = pd.read_sql_query(
            """SELECT * FROM beach_risk_scores
               WHERE computed_at = ?
               ORDER BY island, beach_name, day_offset""",
            conn,
            params=(computed_at,),
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=300)
def load_webcam_latest(db_path: str) -> pd.DataFrame:
    """Retourne la dernière capture réussie par caméra (island, camera_name, file_path, captured_at)."""
    try:
        conn = get_connection(db_path)
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(
            """SELECT island, camera_name, file_path, captured_at, file_size
               FROM webcam_captures
               WHERE success = 1
                 AND file_path IS NOT NULL
                 AND id IN (
                     SELECT MAX(id) FROM webcam_captures
                     WHERE success = 1
                     GROUP BY camera_name
                 )
               ORDER BY island, camera_name""",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


# ── Helpers carte ─────────────────────────────────────────────────────────────

def day_color(offset: int) -> str:
    """Retourne la couleur HEX pour un jour donné (0=vert, 5=rouge)."""
    return DAY_COLORS.get(offset, "#808080")


def make_arrow_icon(degrees: float, color: str) -> DivIcon:
    """
    Crée un DivIcon Folium avec une flèche SVG pivotée selon la direction
    des courants (0° = Est, 90° = Nord en convention mathématique).
    La convention affichage : on veut montrer où va le courant.
    """
    # degrees est en convention mathématique (0=Est, 90=Nord)
    # Pour SVG, on tourne dans le sens horaire depuis le Nord
    # Conversion : svg_deg = 90 - math_deg
    svg_deg = (90 - degrees) % 360

    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
      <g transform="rotate({svg_deg:.1f}, 12, 12)">
        <polygon points="12,2 18,20 12,16 6,20" fill="{color}" stroke="white" stroke-width="1"/>
      </g>
    </svg>
    """
    return DivIcon(
        html=svg,
        icon_size=(24, 24),
        icon_anchor=(12, 12),
    )


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🌊 Sargasses")
    st.divider()

    db_path = st.text_input("Base de données", value="./sargassum_data.db")

    if st.button("Rafraîchir", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    last_collected = get_last_collected(db_path)
    st.caption(f"Dernière collecte : {last_collected}")

    st.divider()
    page = st.radio(
        "Navigation",
        ["Carte", "Métriques", "Actualités", "Plages", "Webcams"],
        label_visibility="collapsed",
    )


# ── Page 1 : Carte de surveillance ───────────────────────────────────────────

if page == "Carte":
    st.header("Carte de surveillance")

    # Bannière NOAA SIR
    df_sir = load_noaa_sir(db_path)
    if not df_sir.empty:
        sir_row = df_sir.iloc[0]
        sir_date = sir_row.get("report_date", "—")
        sir_url  = sir_row.get("report_url", "")
        if sir_url:
            st.info(f"Rapport NOAA SIR du {sir_date} — [Ouvrir le PDF]({sir_url})")
        else:
            st.info(f"Rapport NOAA SIR du {sir_date}")
    else:
        st.warning("Aucun rapport NOAA SIR disponible.")

    # Données de dérive
    df_drift = load_drift(db_path)

    # Sélecteur de jour
    available_days = sorted(df_drift["day_offset"].unique().tolist()) if not df_drift.empty else list(range(6))
    day_labels = [f"j+{d}" for d in available_days]

    if available_days:
        selected_label = st.radio(
            "Snapshot de dérive",
            day_labels,
            horizontal=True,
            label_visibility="collapsed",
        )
        selected_day = available_days[day_labels.index(selected_label)]
    else:
        st.warning("Aucune simulation de dérive disponible.")
        selected_day = 0

    # Construction de la carte Folium
    m = Map(location=[18, -72], zoom_start=5, tiles="CartoDB positron")

    # Couche particules de dérive
    if not df_drift.empty:
        snapshot = df_drift[df_drift["day_offset"] == selected_day]
        if not snapshot.empty:
            fg_particles = FeatureGroup(name=f"Particules {selected_label}", show=True)
            positions_raw = snapshot.iloc[0].get("positions_json", "[]") or "[]"
            try:
                positions = json.loads(positions_raw)
            except (json.JSONDecodeError, TypeError):
                positions = []

            color = day_color(selected_day)
            # positions_json = [[lon, lat], ...] → CircleMarker attend (lat, lon)
            for pt in positions[:500]:
                if len(pt) >= 2:
                    lon, lat = pt[0], pt[1]
                    CircleMarker(
                        location=[lat, lon],
                        radius=3,
                        color=color,
                        fill=True,
                        fill_color=color,
                        fill_opacity=0.7,
                        weight=0,
                    ).add_to(fg_particles)
            fg_particles.add_to(m)

    # Couche courants Copernicus
    df_cop = load_copernicus(db_path)
    if not df_cop.empty:
        fg_cop = FeatureGroup(name="Courants Copernicus", show=True)
        cop_row = df_cop.iloc[-1]  # dernière mesure
        cop_dir = cop_row.get("dominant_dir_deg")
        cop_spd = cop_row.get("mean_speed")
        cop_date = cop_row.get("data_date", "—")
        if cop_dir is not None:
            popup_html = (
                f"<b>Copernicus</b><br>"
                f"Date : {cop_date}<br>"
                f"Vitesse moy : {cop_spd:.3f} m/s<br>"
                f"Direction : {cop_dir:.1f}°"
            ) if cop_spd is not None else f"<b>Copernicus</b><br>Direction : {cop_dir:.1f}°"
            Marker(
                location=[16.0, -72.0],
                icon=make_arrow_icon(cop_dir, "#1a6fdb"),
                popup=Popup(popup_html, max_width=200),
                tooltip="Courant Copernicus",
            ).add_to(fg_cop)
        fg_cop.add_to(m)

    # Couche courants AVISO
    df_av = load_aviso(db_path)
    if not df_av.empty:
        fg_av = FeatureGroup(name="Courants AVISO géostrophiques", show=True)
        av_row = df_av.iloc[-1]
        av_dir = av_row.get("dominant_dir_deg")
        av_spd = av_row.get("mean_speed")
        av_date = av_row.get("data_date", "—")
        if av_dir is not None:
            popup_html = (
                f"<b>AVISO géostrophique</b><br>"
                f"Date : {av_date}<br>"
                f"Vitesse moy : {av_spd:.3f} m/s<br>"
                f"Direction : {av_dir:.1f}°"
            ) if av_spd is not None else f"<b>AVISO</b><br>Direction : {av_dir:.1f}°"
            Marker(
                location=[16.0, -71.5],  # légèrement décalé en longitude
                icon=make_arrow_icon(av_dir, "#e07800"),
                popup=Popup(popup_html, max_width=200),
                tooltip="Courant AVISO géostrophique",
            ).add_to(fg_av)
        fg_av.add_to(m)

    LayerControl(collapsed=False).add_to(m)

    # Rendu de la carte
    st_folium(m, width="100%", height=550, returned_objects=[])

    # Légende des couleurs
    st.markdown("**Légende des snapshots de dérive**")
    cols = st.columns(6)
    for i, (offset, color) in enumerate(DAY_COLORS.items()):
        with cols[i]:
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:6px;">'
                f'<div style="width:16px;height:16px;background:{color};'
                f'border-radius:3px;flex-shrink:0;"></div>'
                f'<span>j+{offset}</span></div>',
                unsafe_allow_html=True,
            )


# ── Page 2 : Métriques & Tendances ───────────────────────────────────────────

elif page == "Métriques":
    st.header("Métriques & Tendances")

    df_afai = load_afai(db_path)
    df_cop  = load_copernicus(db_path)
    df_av   = load_aviso(db_path)
    df_drift = load_drift(db_path)

    # ── KPI cards ──────────────────────────────────────────────────────────────
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    with kpi1:
        if not df_afai.empty and "coverage_pct" in df_afai.columns:
            latest_cov = df_afai["coverage_pct"].iloc[-1]
            delta_cov = None
            if len(df_afai) >= 2:
                delta_cov = round(latest_cov - df_afai["coverage_pct"].iloc[-2], 2)
            st.metric(
                "Couverture AFAI (%)",
                f"{latest_cov:.2f}%",
                delta=f"{delta_cov:+.2f}%" if delta_cov is not None else None,
            )
        else:
            st.metric("Couverture AFAI (%)", "—")

    with kpi2:
        if not df_cop.empty and "mean_speed" in df_cop.columns:
            cop_spd = df_cop["mean_speed"].iloc[-1]
            st.metric("Vitesse Copernicus (m/s)", f"{cop_spd:.3f}")
        else:
            st.metric("Vitesse Copernicus (m/s)", "—")

    with kpi3:
        if not df_av.empty and "mean_speed" in df_av.columns:
            av_spd = df_av["mean_speed"].iloc[-1]
            st.metric("Vitesse AVISO géost. (m/s)", f"{av_spd:.3f}")
        else:
            st.metric("Vitesse AVISO géost. (m/s)", "—")

    with kpi4:
        if not df_drift.empty:
            day0 = df_drift[df_drift["day_offset"] == 0]
            if not day0.empty:
                n_part = day0.iloc[0].get("n_particles", 0) or 0
                act_frac = day0.iloc[0].get("active_fraction", 0) or 0
                active_count = int(n_part * act_frac)
                st.metric("Particules actives j+0", f"{active_count:,}")
            else:
                st.metric("Particules actives j+0", "—")
        else:
            st.metric("Particules actives j+0", "—")

    st.divider()

    # ── Graphique 1 : AFAI coverage_pct ────────────────────────────────────────
    st.subheader("Couverture sargasses AFAI 7-jours")
    if df_afai.empty or "coverage_pct" not in df_afai.columns:
        st.warning("Pas de données AFAI disponibles.")
    elif len(df_afai) == 1:
        st.info(
            f"Une seule mesure disponible ({df_afai['data_date'].iloc[0]}) : "
            f"{df_afai['coverage_pct'].iloc[0]:.2f}%"
        )
    else:
        mean_cov = df_afai["coverage_pct"].mean()
        fig_afai = px.line(
            df_afai,
            x="data_date",
            y="coverage_pct",
            labels={"data_date": "Date", "coverage_pct": "Couverture (%)"},
            markers=True,
        )
        fig_afai.add_hline(
            y=mean_cov,
            line_dash="dot",
            annotation_text=f"Moyenne : {mean_cov:.2f}%",
            annotation_position="bottom right",
            line_color="gray",
        )
        fig_afai.update_layout(hovermode="x unified", height=300)
        st.plotly_chart(fig_afai, width="stretch")

    # ── Graphique 2 : Vitesses Copernicus + AVISO ───────────────────────────────
    st.subheader("Vitesses des courants — Copernicus & AVISO")
    if df_cop.empty and df_av.empty:
        st.warning("Pas de données de courants disponibles.")
    else:
        frames = []
        if not df_cop.empty and "mean_speed" in df_cop.columns and "data_date" in df_cop.columns:
            cop_plot = df_cop[["data_date", "mean_speed"]].copy()
            cop_plot["source"] = "Copernicus"
            cop_plot = cop_plot.rename(columns={"mean_speed": "vitesse_moy_ms"})
            frames.append(cop_plot)
        if not df_av.empty and "mean_speed" in df_av.columns and "data_date" in df_av.columns:
            av_plot = df_av[["data_date", "mean_speed"]].copy()
            av_plot["source"] = "AVISO géostrophique"
            av_plot = av_plot.rename(columns={"mean_speed": "vitesse_moy_ms"})
            frames.append(av_plot)

        if frames:
            df_speeds = pd.concat(frames, ignore_index=True)
            fig_spd = px.line(
                df_speeds,
                x="data_date",
                y="vitesse_moy_ms",
                color="source",
                labels={"data_date": "Date", "vitesse_moy_ms": "Vitesse moy. (m/s)", "source": "Source"},
                markers=True,
                color_discrete_map={
                    "Copernicus": "#1a6fdb",
                    "AVISO géostrophique": "#e07800",
                },
            )
            fig_spd.update_layout(hovermode="x unified", height=300)
            st.plotly_chart(fig_spd, width="stretch")
        else:
            st.warning("Données insuffisantes pour le graphique de vitesses.")


# ── Page 3 : Actualités & Rapports ───────────────────────────────────────────

elif page == "Actualités":
    st.header("Actualités & Rapports")

    # ── NOAA SIR ───────────────────────────────────────────────────────────────
    st.subheader("Rapports NOAA SIR")
    df_sir = load_noaa_sir(db_path)
    if df_sir.empty:
        st.warning("Aucun rapport NOAA SIR disponible.")
    else:
        sir_row = df_sir.iloc[0]
        sir_date = sir_row.get("report_date", "—")
        sir_url  = sir_row.get("report_url", "")

        col_info, col_btn = st.columns([3, 1])
        with col_info:
            st.markdown(f"**Rapport du {sir_date}**")
            if sir_url:
                st.markdown(f"[{sir_url}]({sir_url})")
        with col_btn:
            if sir_url:
                st.link_button("Télécharger le PDF", url=sir_url, width="stretch")

        extra_raw = sir_row.get("extra_files", "[]") or "[]"
        try:
            extra_files = json.loads(extra_raw)
        except (json.JSONDecodeError, TypeError):
            extra_files = []

        if extra_files:
            with st.expander("Fichiers associés (KMZ, CSV…)"):
                for furl in extra_files:
                    st.markdown(f"- [{furl}]({furl})")

    st.divider()

    # ── FORESEA CNRS ───────────────────────────────────────────────────────────
    st.subheader("Prévisions FORESEA CNRS")
    df_foresea = load_foresea(db_path)
    if df_foresea.empty:
        st.warning("Aucune donnée FORESEA disponible.")
    else:
        f_row = df_foresea.iloc[0]
        post_date = f_row.get("latest_post_date")
        post_title = f_row.get("latest_post_title", "")
        snippet = f_row.get("forecast_snippet", "") or ""

        if post_date:
            st.caption(f"Dernier post : {post_date}")
        if post_title:
            st.markdown(f"**{post_title}**")
        if snippet:
            st.text_area(
                "Extrait de prévision",
                value=snippet,
                disabled=True,
                height=160,
                label_visibility="collapsed",
            )
        else:
            st.info("Aucun extrait de prévision disponible.")

        # Liens produits
        links_raw = f_row.get("product_links", "[]") or "[]"
        try:
            links = json.loads(links_raw)
        except (json.JSONDecodeError, TypeError):
            links = []

        if links:
            with st.expander("Liens vers les données"):
                for lnk in links:
                    url  = lnk.get("url", "")
                    text = lnk.get("text", url)
                    if url:
                        st.markdown(f"- [{text}]({url})")

    st.divider()

    # ── Sargassum Monitoring ────────────────────────────────────────────────────
    st.subheader("Sargassum Monitoring")
    df_mon = load_sargassum_monitoring(db_path)
    if df_mon.empty:
        st.warning("Aucun article Sargassum Monitoring disponible.")
    else:
        for _, art in df_mon.iterrows():
            title   = art.get("title",   "") or ""
            date    = art.get("date",    "") or ""
            excerpt = art.get("excerpt", "") or ""
            url     = art.get("url",     "") or ""

            with st.container(border=True):
                header = f"**[{title}]({url})**" if url else f"**{title}**"
                cols_h = st.columns([4, 1])
                with cols_h[0]:
                    st.markdown(header)
                with cols_h[1]:
                    if date:
                        st.caption(date)
                if excerpt:
                    st.write(html.unescape(excerpt))


# ── Page 4 : Risque plages Saint-Barth ───────────────────────────────────────

elif page == "Plages":
    st.header("Risque sargasses — Plages de Saint-Barth")

    df_beaches = load_beach_scores(db_path)

    if df_beaches.empty:
        st.warning(
            "Aucun score de plage disponible. "
            "Lancez d'abord : `python beaches.py`"
        )
    else:
        computed_at  = df_beaches["computed_at"].iloc[0]
        simulated_at = df_beaches["simulated_at"].iloc[0]
        n_particles  = int(df_beaches["n_particles"].iloc[0] or 0)
        st.caption(
            f"Scores calculés : {computed_at} | "
            f"Simulation : {simulated_at} | "
            f"{n_particles:,} particules semées"
        )

        # Sélecteur d'île
        if "island" in df_beaches.columns and df_beaches["island"].notna().any():
            available_islands = sorted(df_beaches["island"].dropna().unique().tolist())
        else:
            available_islands = ["Saint-Barth"]
        selected_island = st.selectbox("Île", available_islands)
        df_beaches = df_beaches[df_beaches["island"] == selected_island].copy() if "island" in df_beaches.columns else df_beaches
        map_cfg = ISLAND_MAP_CONFIG.get(selected_island, ISLAND_MAP_CONFIG["Saint-Barth"])

        # Sélecteur de jour
        available_days = sorted(df_beaches["day_offset"].unique().tolist())
        day_labels = [f"j+{d}" for d in available_days]
        selected_label = st.radio(
            "Jour de prévision",
            day_labels,
            horizontal=True,
            label_visibility="collapsed",
        )
        selected_day = available_days[day_labels.index(selected_label)]
        df_day = df_beaches[df_beaches["day_offset"] == selected_day].copy()

        col_map, col_matrix = st.columns([1.2, 1])

        # ── Carte Folium Saint-Barth ────────────────────────────────────────────
        with col_map:
            st.subheader(f"Carte — {selected_label}")
            m_beach = Map(
                location=map_cfg["center"],
                zoom_start=map_cfg["zoom"],
                tiles="CartoDB positron",
            )
            for _, brow in df_day.iterrows():
                color = RISK_COLORS.get(brow["risk_level"], "#808080")
                reg   = brow.get("regional_score") or 0
                loc   = brow.get("local_score")    or 0
                prox  = brow.get("closest_km")
                prox_str = f"{prox:.1f} km" if prox is not None else "—"
                popup_html = (
                    f"<b>{brow['beach_name']}</b><br>"
                    f"Risque : <b>{brow['risk_level']}</b><br>"
                    f"Score régional (σ=50 km) : {reg:.1f}<br>"
                    f"Score local (σ=r) : {loc:.2f}<br>"
                    f"Particule la plus proche : {prox_str}<br>"
                    f"Rayon catchment : {brow['radius_km']:.0f} km"
                )
                CircleMarker(
                    location=[brow["beach_lat"], brow["beach_lon"]],
                    radius=10,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.85,
                    weight=2,
                    popup=Popup(popup_html, max_width=240),
                    tooltip=f"{brow['beach_name']} — {brow['risk_level']} — {prox_str}",
                ).add_to(m_beach)
            st_folium(m_beach, width="100%", height=430, returned_objects=[])

        # ── Matrice de risque (heatmap) ────────────────────────────────────────
        with col_matrix:
            st.subheader("Matrice de risque")

            # regional_score : valeur continue — couleur proportionnelle aux seuils
            pivot_reg = df_beaches.pivot_table(
                index="beach_name", columns="day_offset",
                values="regional_score", aggfunc="first",
            )
            pivot_reg.columns = [f"j+{c}" for c in pivot_reg.columns]

            pivot_prox = df_beaches.pivot_table(
                index="beach_name", columns="day_offset",
                values="closest_km", aggfunc="first",
            )
            pivot_prox.columns = [f"j+{c}" for c in pivot_prox.columns]

            # Texte de cellule : "17.9 / 36km"
            text_vals = pivot_reg.round(1).astype(str) + "<br>" + pivot_prox.round(0).astype(int).astype(str) + " km"

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot_reg.values,
                x=pivot_reg.columns.tolist(),
                y=pivot_reg.index.tolist(),
                colorscale=[
                    [0.000, "#00c800"],   # none  < 5
                    [0.067, "#c8c800"],   # low   ≥ 5   (5/75)
                    [0.333, "#c86400"],   # medium ≥ 25 (25/75)
                    [1.000, "#c80000"],   # high  ≥ 75
                ],
                zmin=0, zmax=75,
                text=text_vals.values,
                texttemplate="%{text}",
                showscale=True,
                colorbar=dict(title="Score<br>régional", thickness=14),
                hovertemplate=(
                    "<b>%{y}</b><br>%{x}<br>"
                    "regional_score : %{z:.1f}<extra></extra>"
                ),
            ))
            fig_heat.update_layout(
                height=390,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_heat, width="stretch")

            # Légende
            legend_items = [
                ("aucun",  RISK_COLORS["none"]),
                ("faible", RISK_COLORS["low"]),
                ("moyen",  RISK_COLORS["medium"]),
                ("élevé",  RISK_COLORS["high"]),
            ]
            st.markdown(
                '<div style="display:flex;gap:14px;flex-wrap:wrap;">'
                + "".join(
                    f'<span style="display:flex;align-items:center;gap:5px;">'
                    f'<span style="width:14px;height:14px;background:{c};'
                    f'border-radius:3px;flex-shrink:0;"></span>'
                    f'<small>{label}</small></span>'
                    for label, c in legend_items
                )
                + "</div>",
                unsafe_allow_html=True,
            )

        # ── Tableau détaillé ───────────────────────────────────────────────────
        st.divider()
        st.subheader(f"Détail — {selected_label}")

        detail = (
            df_day[["beach_name", "risk_level", "regional_score",
                     "local_score", "closest_km", "radius_km", "est_count"]]
            .rename(columns={
                "beach_name":      "Plage",
                "risk_level":      "Risque",
                "regional_score":  "Score régional (σ=50km)",
                "local_score":     "Score local (σ=r)",
                "closest_km":      "Particule la + proche (km)",
                "radius_km":       "Rayon (km)",
                "est_count":       "Ptcl. estimées",
            })
            .reset_index(drop=True)
        )
        st.dataframe(detail, width="stretch", hide_index=True)


# ── Page 5 : Webcams ──────────────────────────────────────────────────────────

elif page == "Webcams":
    from pathlib import Path as _Path

    st.header("Webcams — Dernières captures")

    df_cams = load_webcam_latest(db_path)

    if df_cams.empty:
        st.warning(
            "Aucune capture disponible. "
            "Lancez d'abord : `python sargassum_webcam_capture.py --once`"
        )
    else:
        for island in df_cams["island"].unique():
            st.subheader(island)
            island_df = df_cams[df_cams["island"] == island].reset_index(drop=True)
            cols = st.columns(3)
            for i, row in island_df.iterrows():
                with cols[i % 3]:
                    img_path = _Path(row["file_path"])
                    if img_path.exists():
                        caption = (
                            f"{row['camera_name']}  ·  "
                            f"{row['captured_at'][:16].replace('T', ' ')}"
                        )
                        st.image(str(img_path), caption=caption, width="stretch")
                    else:
                        st.warning(f"**{row['camera_name']}**  \nFichier introuvable")

        st.divider()
        with st.expander("Historique des captures (24 dernières heures)"):
            conn_hist = get_connection(db_path)
            if conn_hist:
                try:
                    df_hist = pd.read_sql_query(
                        """SELECT captured_at, island, camera_name,
                                  success, http_status, file_size
                           FROM webcam_captures
                           WHERE captured_at >= datetime('now', '-24 hours')
                           ORDER BY captured_at DESC
                           LIMIT 200""",
                        conn_hist,
                    )
                    conn_hist.close()
                    if not df_hist.empty:
                        st.dataframe(df_hist, width="stretch", hide_index=True)
                    else:
                        st.info("Aucune capture dans les dernières 24 heures.")
                except Exception:
                    st.warning("Impossible de charger l'historique.")
