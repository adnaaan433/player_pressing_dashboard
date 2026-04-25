import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import font_manager
from mplsoccer import VerticalPitch, FontManager, add_image
from matplotlib.colors import LinearSegmentedColormap
from urllib.request import urlopen
import requests
from highlight_text import ax_text
from PIL import Image

from function import get_event_data, get_processed_data, get_team_stats, fetch_player_stats, filter_player_stats, player_viz, team_pressing_viz, get_competitions, get_season_teams, save_league_zonewise_stats
import streamlit as st
import os

st.set_page_config(page_title="Player Pressing Dashboard", page_icon=":soccer:")

# --- Simple Password Authentication System ---
APP_PASSWORD = st.secrets.get("app_password", "YOUR_DEFAULT_PASSWORD") # Will read from secrets

# Create a container so the password UI looks clean
auth_container = st.container()
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    with auth_container:
        st.title("🔒 Restricted Access")
        st.write("Please enter the password to view the Player Pressing Dashboard.")
        password_input = st.text_input("Password", type="password")
        
        if st.button("Unlock"):
            if password_input == APP_PASSWORD:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password.")
        st.stop() # Stops the rest of the app from loading until authenticated
# ---------------------------------------------


# Set environment variables from secrets for use in function.py and Sbapi
if "statsbomb" in st.secrets:
    os.environ["SB_USERNAME"] = st.secrets["statsbomb"]["username"]
    os.environ["SB_PASSWORD"] = st.secrets["statsbomb"]["password"]


st.sidebar.title("Navigation")
# page = st.sidebar.radio("Go to", ["Dashboard", "League Data Downloader"])
page = "Dashboard"

# if page == "League Data Downloader":
#     st.title("Download League Data")
#     st.write("Download entire season data for all teams and calculate zonewise stats.")
#     
#     competitions_df = get_competitions()
#     competition_names = competitions_df['competition_name'].unique()
#     selected_league_name = st.selectbox("Select competition", competition_names, index=0, key="dl_comp")
#     
#     seasons_df = competitions_df[competitions_df['competition_name'] == selected_league_name]
#     season_names = seasons_df['season_name'].unique()
#     selected_season_name = st.selectbox("Select season", season_names, index=0, key="dl_seas")
#     
#     selected_row = seasons_df[seasons_df['season_name'] == selected_season_name].iloc[0]
#     competition_id = int(selected_row['competition_id'])
#     season_id = int(selected_row['season_id'])
#     
#     if st.button("Download & Save League Data"):
#         progress_bar = st.progress(0)
#         status_text = st.empty()
#         
#         def update_progress(progress, message):
#             progress_bar.progress(progress)
#             status_text.text(message)
#             
#         df_league = get_event_data(season_id, competition_id, team_name="All Teams", progress_callback=update_progress)
#         status_text.text("Processing zonewise pressing stats...")
#         csv_file = save_league_zonewise_stats(df_league, ".", selected_league_name, selected_season_name)
#         status_text.empty()
#         progress_bar.empty()
#         st.success(f"Successfully saved zonewise stats to {csv_file}")

if page == "Dashboard":
    st.title("Player Pressing Dashboard")

    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False

    # Robustness check: If code updated and pdf_raw is missing but data_loaded is True, reset.
    if st.session_state.data_loaded and 'pdf_raw' not in st.session_state:
        st.session_state.data_loaded = False

    # Fetch available competitions dynamically
    competitions_df = get_competitions()

    # Select Competition(s)
    competition_names = competitions_df['competition_name'].unique()
    selected_league_names = st.multiselect("Select competition(s)", competition_names, default=[competition_names[0]])

    if not selected_league_names:
        st.warning("Please select at least one competition.")
        st.stop()

    # Filter for seasons based on selected competitions
    seasons_df = competitions_df[competitions_df['competition_name'].isin(selected_league_names)]
    season_names = seasons_df['season_name'].unique()
    selected_season_name = st.selectbox("Select season", season_names, index=0)

    # Get the combinations of competition and season IDs
    selected_rows = seasons_df[seasons_df['season_name'] == selected_season_name]

    # Fetch available teams for the selected season and competitions
    team_names = []
    team_comp_map = {}
    for _, row in selected_rows.iterrows():
        c_id = int(row['competition_id'])
        s_id = int(row['season_id'])
        c_name = row['competition_name']
        teams = get_season_teams(s_id, c_id)
        if teams:
            for t in teams:
                team_names.append(t)
                team_comp_map[t] = (c_id, s_id, c_name)
    
    if not team_names:
        st.warning("No teams found for the selected combination.")
        st.stop()

    team_names = sorted(list(set(team_names)))
    selected_team_name = st.selectbox("Select Team", team_names, index=0)

    # --- LOAD / CLEAR DATA BUTTONS ---
    btn_col1, btn_col2 = st.columns([1, 1])

    with btn_col1:
        load_clicked = st.button("Load Team Data", key="load_data_button")

    with btn_col2:
        if st.session_state.get("data_loaded") and "df" in st.session_state:
            if st.button("🗑️ Clear Event Data", key="clear_event_data_button",
                         help="Remove the loaded event data from memory. Player & team stats remain cached."):
                del st.session_state.df
                st.session_state.data_loaded = False
                st.success("Event data cleared. Player & team stats are still cached.")
                st.rerun()

    if load_clicked:
        # Create progress bar and status text
        progress_bar = st.progress(0)
        status_text = st.empty()
    
        def update_progress(progress, message):
            progress_bar.progress(progress)
            status_text.text(message)
        
        event_comp_id, event_season_id, event_comp_name = team_comp_map[selected_team_name]

        # Fetch Event Data for specific team from its specific competition
        st.session_state.df = get_event_data(event_season_id, event_comp_id, team_name=selected_team_name, progress_callback=update_progress)
    
        # Fetch Raw Player Stats (All players in selected competitions)
        status_text.text("Fetching player stats...")
        player_dfs = []
        for _, row in selected_rows.iterrows():
            c_id = int(row['competition_id'])
            s_id = int(row['season_id'])
            p_df = fetch_player_stats(season_id=s_id, competition_id=c_id)
            if p_df is not None and not p_df.empty:
                player_dfs.append(p_df)
        st.session_state.pdf_raw = pd.concat(player_dfs, ignore_index=True) if player_dfs else pd.DataFrame()
    
        # Fetch Team Stats
        status_text.text("Fetching team stats...")
        team_dfs = []
        for _, row in selected_rows.iterrows():
            c_id = int(row['competition_id'])
            s_id = int(row['season_id'])
            t_df = get_team_stats(season_id=s_id, competition_id=c_id)
            if t_df is not None and not t_df.empty:
                team_dfs.append(t_df)
        if team_dfs:
            combined_team_stats = pd.concat(team_dfs, ignore_index=True)
            st.session_state.team_stats = combined_team_stats.groupby('team_name', as_index=False)['possession'].mean()
        else:
            st.session_state.team_stats = pd.DataFrame()
    
        # Store Context
        st.session_state.df_teamNameId = pd.read_csv("teams_name_and_id_Statsbomb_Names.csv")
        st.session_state.selected_league = ", ".join(selected_league_names) if len(selected_league_names) <= 2 else "Multiple Competitions"
        st.session_state.selected_event_league = event_comp_name
        st.session_state.selected_season = selected_season_name
        st.session_state.selected_team_name = selected_team_name
        st.session_state.data_loaded = True
    
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        st.success("Data Loaded Successfully!")

    # --- FILTERS & VISUALIZATION ---
    if st.session_state.data_loaded:
        st.divider()
        # view_mode = st.radio("Select View Mode", ["Player Pressing", "Team Pressing Heatmap"], horizontal=True)
        view_mode = "Player Pressing"

        if view_mode == "Player Pressing":
            st.subheader("Filter Players")
            col1, col2 = st.columns(2)
        
            with col1:
                position_choice = st.selectbox("Select position filter", ['CF', 'Winger', 'AM/CM', 'DM', 'FB', 'CB', 'GK'], index=0)
            with col2:
                minimum_minutes_choice = st.slider("Minimum minutes played", min_value=0, max_value=2000, value=500, step=100)
        
            possession_adjusted = st.toggle("Possession Adjusted Calculations", value=True)
            
            show_padj_stats_text = False
            if possession_adjusted:
                show_padj_stats_text = st.toggle("Show Possession Adjusted Stats in text", value=True)
        
            # Apply filters to raw data
            filtered_pdf = filter_player_stats(st.session_state.pdf_raw, minimum_minutes_choice, position_choice)
        
            if filtered_pdf.empty:
                st.warning("No players found with these filters.")
            else:
                # Calculate percentiles on the filtered group
                st.session_state.pdf = get_processed_data(filtered_pdf, st.session_state.team_stats, possession_adjusted)
            
                # Store selections for text
                st.session_state.position_choice = position_choice
                st.session_state.minimum_minutes_choice = minimum_minutes_choice
            
                st.divider()

                # Choose a player
                player_name = st.selectbox("Select Player", sorted(st.session_state.pdf['player_name'].dropna().unique()), key="player_selectbox")

                # Load custom fonts
                try:
                    font_bold = font_manager.FontProperties(fname=os.path.abspath('Boldonse-Regular.ttf'))
                    font_regular = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Regular.ttf'))
                except Exception as e:
                    print(f"Error loading fonts: {e}")
                    font_bold = None
                    font_regular = None

                # Generate visualization
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10), gridspec_kw={'width_ratios': [1, 1], 'wspace': -0.1})
                pkname, pltime, ftmb_tid, overall_percentile, overall_color = player_viz(st.session_state.df, st.session_state.pdf, st.session_state.df_teamNameId, player_name, ax1, ax2, possession_adjusted, show_padj_stats_text, selected_team=st.session_state.selected_team_name)
            
                # Title (Player Name) - Boldonse
                fig.text(0.24, 1.11, pkname, fontsize=28, fontproperties=font_bold)
            
                # Subtitles - NotoSans
                fig.text(0.24, 1.06, f'Off the ball workrate, for {st.session_state.selected_team_name} in {st.session_state.selected_season} season | Data: Statsbomb', color='#0f0f0f', fontsize=15, fontproperties=font_regular)
                top_5 = {'1. Bundesliga', 'La Liga', 'Premier League', 'Serie A', 'Ligue 1'}
                if set(selected_league_names) == top_5:
                    league_label = 'Top 5 Leagues'
                elif len(selected_league_names) == 1:
                    league_label = selected_league_names[0]
                else:
                    league_label = st.session_state.selected_league
                fig.text(0.24, 1.02, f'Percentile among {league_label} {st.session_state.position_choice}s with {st.session_state.minimum_minutes_choice}+ minutes played | Made by: @adnaaan433', color='#202020', fontsize=15, fontproperties=font_regular)
            
                if ftmb_tid:
                    try:
                        himage = urlopen(f"https://images.fotmob.com/image_resources/logo/teamlogo/{ftmb_tid}.png")
                        himage = Image.open(himage)
                        ax_himage = add_image(himage, fig, left=0.13, bottom=1.02, width=0.12, height=0.12)
                    except Exception:
                        pass
            
                # Add Overall Percentile Scatter Top-Right of Figure
                # Create a small new axes precisely planted in the top right
                ax_scatter = fig.add_axes([0.8, 1.03, 0.1, 0.1]) # [left, bottom, width, height]
                ax_scatter.axis('off') # Hide its background
                ax_scatter.scatter(0.5, 0.5, s=5000, color=overall_color, edgecolor='None', zorder=10)
                
                # Check if we should use font_con_bold, fallback to font_bold if missing here
                try:
                    font_con_bold = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Bold.ttf'))
                except Exception:
                    font_con_bold = font_bold

                ax_scatter.text(0.5, 0.5, f'{int(overall_percentile)}', color='white', va='center', ha='center', zorder=11, fontproperties=font_con_bold, fontsize=35)
                # ax_scatter.text(0.5, 0.25, 'OVERALL\nPERCENTILE', color='#808080', va='center', ha='center', fontsize=12, zorder=11, fontproperties=font_bold)

                plt.tight_layout()
                st.pyplot(fig)
                st.text(f"Minutes played: {pltime}")

                st.subheader(f"Overall Percentiles: Top 5 Leagues {st.session_state.position_choice}")
                # Print the dataframe, ranked by the newly computed overall percentile
                display_df = st.session_state.pdf.sort_values(by='overall_percentile', ascending=False).reset_index(drop=True)
                st.dataframe(display_df, use_container_width=True)

                st.divider()
                col_title, col_clear = st.columns([3, 1])
                with col_title:
                    st.subheader("Compare Players Scatter Plot")
                with col_clear:
                    st.write("") # Spacing
                    if st.button("Clear Selections"):
                        if 'accumulated_players' in st.session_state:
                            st.session_state.accumulated_players.clear()
                        if 'last_scatter_click' in st.session_state:
                            st.session_state.last_scatter_click = None
                            
                stats_options = [
                    'defensive_actions_90', 'defensive_action_regains_90', 
                    'pressures_90', 'pressure_regains_90', 
                    'counterpressures_90', 'counterpressure_regains_90',
                    'pressured_passing_ratio'
                ]
                
                # Check if all options exist in the dataframe to avoid errors
                available_options = [opt for opt in stats_options if opt in display_df.columns]
                
                if len(available_options) >= 2:
                    col_x, col_y = st.columns(2)
                    with col_x:
                        x_axis = st.selectbox("Select X-axis Stat", available_options, index=0)
                    with col_y:
                        y_axis = st.selectbox("Select Y-axis Stat", available_options, index=1)
                    
                    import plotly.express as px
                    
                    if 'accumulated_players' not in st.session_state:
                        st.session_state.accumulated_players = set()
                    if 'last_scatter_click' not in st.session_state:
                        st.session_state.last_scatter_click = None
                        
                    # Check clicked players from session state
                    sel = st.session_state.get("scatter_click")
                    if sel != st.session_state.last_scatter_click:
                        st.session_state.last_scatter_click = sel
                        if sel and "selection" in sel and sel["selection"]["points"]:
                            for pt in sel["selection"]["points"]:
                                p_name = None
                                if "customdata" in pt and len(pt["customdata"]) > 0:
                                    p_name = pt["customdata"][0]
                                elif "hovertext" in pt:
                                    p_name = pt["hovertext"]
                                    
                                if p_name:
                                    if p_name in st.session_state.accumulated_players:
                                        st.session_state.accumulated_players.remove(p_name)
                                    else:
                                        st.session_state.accumulated_players.add(p_name)

                    display_df_scatter = display_df.copy()
                    if st.session_state.accumulated_players:
                        display_df_scatter['is_selected'] = display_df_scatter['player_name'].isin(st.session_state.accumulated_players).astype(str)
                    else:
                        display_df_scatter['is_selected'] = "False"
                        
                    display_df_scatter = display_df_scatter.sort_values('is_selected')
                    
                    median_x = display_df_scatter[x_axis].median()
                    median_y = display_df_scatter[y_axis].median()
                    
                    scatter_title = f"{x_axis.replace('_', ' ').title()} vs {y_axis.replace('_', ' ').title()}"
                    scatter_subtitle = f"{st.session_state.position_choice}s with {st.session_state.minimum_minutes_choice}+ minutes in {league_label} {st.session_state.selected_season} season  |  Data: Statsbomb  |  made by: @adnaaan433"
                    
                    fig_scatter = px.scatter(
                        display_df_scatter, 
                        x=x_axis, 
                        y=y_axis, 
                        hover_name="player_name",
                        hover_data=["team_name", "minutes"],
                        custom_data=["player_name"],
                        color="is_selected",
                        color_discrete_map={"True": "#E452FF", "False": "#5A5A5A"},
                        title=f"{scatter_title}<br><sup style='color:gray'>{scatter_subtitle}</sup>"
                    )
                    fig_scatter.update_traces(marker=dict(size=8, opacity=0.6))
                    fig_scatter.for_each_trace(lambda t: t.update(marker=dict(size=14, opacity=1.0, line=dict(width=2, color='white'))) if t.name == "True" else None)
                    
                    if st.session_state.accumulated_players:
                        for p_name in st.session_state.accumulated_players:
                            if p_name in display_df_scatter['player_name'].values:
                                clicked_row = display_df_scatter[display_df_scatter['player_name'] == p_name].iloc[0]
                                
                                # Use player_known_name if available, else player_name
                                display_name = clicked_row['player_known_name'] if pd.notna(clicked_row['player_known_name']) else clicked_row['player_name']
                                
                                # Format the annotation text
                                annotation_text = f"{display_name}"
                                
                                fig_scatter.add_annotation(
                                    x=clicked_row[x_axis], y=clicked_row[y_axis],
                                    text=annotation_text,
                                    showarrow=False,
                                    yshift=15,
                                    align="center",
                                    font=dict(color="white", size=13)
                                )

                    # Add median lines
                    fig_scatter.add_vline(x=median_x, line_dash="dash", line_color="rgba(128, 128, 128, 0.8)")
                    fig_scatter.add_hline(y=median_y, line_dash="dash", line_color="rgba(128, 128, 128, 0.8)")
                    
                    fig_scatter.update_layout(
                        showlegend=False,
                        width=800,
                        height=800,
                        clickmode='event+select'
                    )
                    
                    # Very low opacity grid
                    fig_scatter.update_xaxes(
                        showgrid=True,
                        gridcolor="rgba(128, 128, 128, 0.1)",
                        zerolinecolor="rgba(128, 128, 128, 0.1)",
                    )
                    fig_scatter.update_yaxes(
                        showgrid=True,
                        gridcolor="rgba(128, 128, 128, 0.1)",
                        zerolinecolor="rgba(128, 128, 128, 0.1)",
                    )
                    
                    st.plotly_chart(
                        fig_scatter, 
                        use_container_width=False, 
                        on_select="rerun", 
                        selection_mode=("points", "box", "lasso"), 
                        key="scatter_click",
                        config={
                            'displayModeBar': True,
                            'toImageButtonOptions': {
                                'format': 'png',
                                'filename': 'player_comparison_scatter',
                                'height': 800,
                                'width': 800,
                                'scale': 2
                            }
                        }
                    )

#         elif view_mode == "Team Pressing Heatmap":
#             st.subheader(f"{selected_team_name} Pressing Heatmap")
#             show_numbers = st.toggle('show numbers', value=True)
# 
#             # Load custom fonts
#             try:
#                 font_bold = font_manager.FontProperties(fname=os.path.abspath('Boldonse-Regular.ttf'))
#                 font_regular = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Regular.ttf'))
#             except Exception as e:
#                 font_bold = None
#                 font_regular = None
# 
#             league_clean = str(st.session_state.selected_event_league).replace(" ", "_").replace("/", "_")
#             season_clean = str(st.session_state.selected_season).replace(" ", "_").replace("/", "_")
#             league_csv = f"{league_clean}_{season_clean}_zonewise_pressing.csv"
#         
#             if os.path.exists(league_csv):
#                 fig = plt.figure(figsize=(10, 11))
#                 from matplotlib.gridspec import GridSpec
#                 gs = GridSpec(2, 3, figure=fig, height_ratios=[120, 12], width_ratios=[20, 80, 20], hspace=0, wspace=0)
#                 ax_left = fig.add_subplot(gs[0, 0])
#                 ax_pitch = fig.add_subplot(gs[0, 1])
#                 ax_bottom = fig.add_subplot(gs[1, 1])
#                 ax_right = fig.add_subplot(gs[0, 2])
#             
#                 import matplotlib.lines as mlines
#                 team_line = mlines.Line2D([], [], color='#E452FF', marker='o', mfc='white', lw=2, label=selected_team_name)
#                 avg_line = mlines.Line2D([], [], color='#808080', marker='o', mfc='white', lw=2, label='League Average')
#                 legend_font = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Regular.ttf'), size=14) if font_regular else {'size': 20}
#                 fig.legend(handles=[team_line, avg_line], loc='lower left', bbox_to_anchor=(0.01, 0.025), prop=legend_font, frameon=False)
#             else:
#                 fig, ax_pitch = plt.subplots(figsize=(8, 10))
#                 ax_left = None
#                 ax_bottom = None
#                 ax_right = None
# 
#             ftmb_tid = team_pressing_viz(st.session_state.df, selected_team_name, st.session_state.df_teamNameId, ax_pitch, ax_left, ax_bottom, ax_right, league_csv if os.path.exists(league_csv) else None, show_numbers=show_numbers)
# 
#             fig.text(0.2, 1.09, f'{selected_team_name} Pressing Heatmap', fontproperties=font_bold, fontsize=18)
#             fig.text(0.2, 1.06, f'Number of Pressing actions per zone of the pitch, in top 5 Leagues {st.session_state.selected_season} season', 
#                     color='#0f0f0f', fontsize=18, ha='left', fontproperties=font_regular)
#             fig.text(0.2, 1.03, f'Data: Statsbomb | Made by: @adnaaan433 | Design: @gusfop', 
#                     color='#0f0f0f', fontsize=18, ha='left', fontproperties=font_regular)
# 
#             if ftmb_tid:
#                 try:
#                     himage = urlopen(f"https://images.fotmob.com/image_resources/logo/teamlogo/{ftmb_tid}.png")
#                     himage = Image.open(himage)
#                     ax_himage = add_image(himage, fig, left=0.025, bottom=0.99, width=0.15, height=0.15)
#                 except Exception:
#                     pass
# 
#             plt.tight_layout()
#             st.pyplot(fig)

        else:
            st.write("Please select the competition, season, and team, then click Load Team Data.")