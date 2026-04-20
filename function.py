import pandas as pd
import requests
import os
import numpy as np
from matplotlib import font_manager
from matplotlib.colors import LinearSegmentedColormap
from highlight_text import ax_text
from mplsoccer import VerticalPitch
from mplsoccer import Sbapi

import streamlit as st
import concurrent.futures

def get_competitions():
    username = os.getenv('SB_USERNAME')
    password = os.getenv('SB_PASSWORD')
    parser = Sbapi(dataframe=True, username=username, password=password)
    return parser.competition()


def get_season_teams(season_id, competition_id):
    username = os.getenv('SB_USERNAME')
    password = os.getenv('SB_PASSWORD')
    parser = Sbapi(dataframe=True, username=username, password=password)
    try:
        df_match = parser.match(competition_id=competition_id, season_id=season_id)
        if df_match.empty:
            return []
        teams = set(df_match['home_team_name'].dropna()).union(set(df_match['away_team_name'].dropna()))
        return sorted(list(teams))
    except:
        return []

def fetch_single_match(parser, mid, team_name=None):
    try:
        # parser.event returns a tuple of (events, related, freeze, tactics)
        # We only need the first element (events dataframe)
        event_data = parser.event(mid)
        if isinstance(event_data, tuple):
            df_event = event_data[0]
        else:
            df_event = event_data
            
        cols = ['id', 'match_id', 'type_name', 'outcome_name',
               'play_pattern_name', 'team_name', 'player_name', 'player_position_name',
               'x', 'y', 'under_pressure', 'counterpress']
        # Ensure columns exist before selecting
        existing_cols = [c for c in cols if c in df_event.columns]
        df_event = df_event[existing_cols]
        if team_name and team_name != "All Teams":
            df_event = df_event[(df_event['type_name'] == 'Pressure') & (df_event['team_name'] == team_name)]
        else:
            df_event = df_event[df_event['type_name'] == 'Pressure']
        return df_event
    except Exception:
        return None

def get_event_data(season_id, competition_id, team_name=None, progress_callback=None):
    username = os.getenv('SB_USERNAME')
    password = os.getenv('SB_PASSWORD')
    parser = Sbapi(dataframe=True, username=username, password=password)
    
    try:
        df_match = parser.match(competition_id=competition_id, season_id=season_id)
        
        if df_match.empty:
            return pd.DataFrame(columns=['player_name', 'type_name', 'counterpress', 'x', 'y'])
            
        df_match = df_match[['match_id', 'home_team_name', 'away_team_name', 'match_status']]
        
        # Filter by Team if provided
        if team_name and team_name != "All Teams":
            df_match = df_match[(df_match['home_team_name'] == team_name) | (df_match['away_team_name'] == team_name)]
            
        status_filter = ['Completed', 'available', 'Available']
        df_match = df_match[df_match['match_status'].isin(status_filter)]
        mids = df_match['match_id'].unique()
        total_matches = len(mids)
        
        if progress_callback:
            progress_callback(0, f"Found {total_matches} matches for {team_name if team_name else 'season'}. Starting parallel download...")
            
    except Exception as e:
        return pd.DataFrame(columns=['player_name', 'type_name', 'counterpress', 'x', 'y'])

    all_event = []
    
    # Use ThreadPoolExecutor to fetch matches in parallel
    # API Limit: 15,000 requests / 5 mins (~50 req/sec). 
    # Increasing workers to 50 to maximize throughput.
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Map each match ID to a future
        future_to_mid = {executor.submit(fetch_single_match, parser, mid, team_name): mid for mid in mids}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_mid)):
            result = future.result()
            if result is not None:
                all_event.append(result)
            
            if progress_callback:
                progress_callback((i + 1) / total_matches, f"Processed {i+1}/{total_matches} matches")

    if all_event:
        df = pd.concat(all_event, ignore_index=True)
        if team_name and team_name != "All Teams":
            df = df[(df['type_name'] == 'Pressure') & (df['team_name'] == team_name)]
        else:
            df = df[df['type_name'] == 'Pressure']
    else:
        df = pd.DataFrame(columns=['player_name', 'type_name', 'counterpress', 'x', 'y'])
    
    return df

@st.cache_data(show_spinner=False)
def fetch_player_stats(season_id, competition_id, username=None, password=None):
    if username is None:
        username = os.getenv('SB_USERNAME')
    if password is None:
        password = os.getenv('SB_PASSWORD')

    url = f"https://data.statsbombservices.com/api/v4/competitions/{competition_id}/seasons/{season_id}/player-stats"
    
    if username and password:
        response = requests.get(url, auth=(username, password))
    else:
        print("Warning: No StatsBomb credentials provided. Attempting public access.")
        response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None
    
    pdf = pd.DataFrame(response.json())

    pdf.columns = pdf.columns.str.replace('player_season_', '', regex=False)
    pdf = pdf[[# player
            'player_name', 'player_known_name', 'team_name',
            'minutes', 'primary_position', 'secondary_position',
            # Stats
            'defensive_actions_90', 'defensive_action_regains_90', 
            'pressures_90', 'pressure_regains_90', 
            'counterpressures_90', 'counterpressure_regains_90']]
    return pdf

def filter_player_stats(pdf, minimum_minutes, position_filter):
    if position_filter == 'CF':
        pf = ['Centre Forward', 'Left Centre Forward', 'Right Centre Forward', 'Secondary Striker']
    elif position_filter == 'Winger':
        pf = ['Left Wing', 'Right Wing', 'Right Attacking Midfielder', 'Left Attacking Midfielder', 'Left Midfielder', 'Right Midfielder']
    elif position_filter == 'AM/CM':
        pf = ['Centre Attacking Midfielder', 'Centre Midfielder', 'Left Centre Midfielder', 'Right Centre Midfielder']
    elif position_filter == 'DM':
        pf = ['Centre Defensive Midfielder', 'Left Defensive Midfielder', 'Right Defensive Midfielder']
    elif position_filter == 'FB':
        pf = ['Left Back', 'Right Back', 'Left Wing Back', 'Right Wing Back']
    elif position_filter == 'CB':
        pf = ['Left Centre Back', 'Right Centre Back', 'Centre Back']
    elif position_filter == 'GK':
        pf = ['Goalkeeper']
    else:
        pf = []
        
    pdf_filtered = pdf[(pdf.minutes >= minimum_minutes) & (pdf.primary_position.isin(pf))].copy()
    return pdf_filtered

@st.cache_data(show_spinner=False)
def get_team_stats(season_id, competition_id, username=None, password=None):
    if username is None:
        username = os.getenv('SB_USERNAME')
    if password is None:
        password = os.getenv('SB_PASSWORD')

    url = f"https://data.statsbombservices.com/api/v4/competitions/{competition_id}/seasons/{season_id}/team-stats"
    
    if username and password:
        response = requests.get(url, auth=(username, password))
    else:
        print("Warning: No StatsBomb credentials provided. Attempting public access.")
        response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None
    
    teamdf = pd.DataFrame(response.json())
    teamdf.columns = teamdf.columns.str.replace('team_season_', '', regex=False)
    teamdf = teamdf[['team_name', 'possession']]
    
    return teamdf

def get_processed_data(pdf, teamdf, possession_adjusted=True):
    # First, merge possession data with pdf
    pdf = pdf.merge(teamdf[['team_name', 'possession']], on='team_name', how='left')

    # Define the stat columns to adjust
    stat_columns = ['defensive_actions_90', 'defensive_action_regains_90', 
                    'pressures_90', 'pressure_regains_90', 
                    'counterpressures_90', 'counterpressure_regains_90']

    import numpy as np
    # Create possession-adjusted columns
    for col in stat_columns:
        if possession_adjusted:
            # Using a softened Sigmoid Possession Adjustment Formula to reduce inflation:
            # Multiplier = 2 / (1 + exp(-0.05 * (Possession% - 50)))
            # The API possession is a decimal (e.g., 0.45), so we multiply by 100
            possession_pct = pdf['possession'] * 100
            multiplier = 2 / (1 + np.exp(-0.05 * (possession_pct - 50)))
            pdf[f'{col}_padj'] = pdf[col] * multiplier
        else:
            # Leave as raw per-90 stats
            pdf[f'{col}_padj'] = pdf[col]

    # Calculate percentiles for possession-adjusted stats
    padj_columns = [f'{col}_padj' for col in stat_columns]
    for col in padj_columns:
        pdf[f'{col}_percentile'] = pdf[col].rank(pct=True) * 100

    # Calculate overall percentile using weighted average
    weights = [0.25, 0.15, 0.2, 0.1, 0.2, 0.1]
    percentile_cols = [f'{col}_percentile' for col in padj_columns]
    # Summing element-wise over columns
    pdf['overall_percentile'] = sum(pdf[col] * w for col, w in zip(percentile_cols, weights))
    
    return pdf


def player_viz(df, pdf, df_teamNameId, pname, ax_pitch, ax_bars, possession_adjusted=True, show_padj_stats_text=True, selected_team=None):
    # Load fonts
    font_bold = font_manager.FontProperties(fname=os.path.abspath('Boldonse-Regular.ttf'))
    font_regular = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Regular.ttf'))
    font_con_bold = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Bold.ttf'))

    pdata = df[df['player_name'] == pname]
    press = pdata[(pdata['type_name'] == 'Pressure') & (pdata['counterpress'].isna())]
    counterpress = pdata[(pdata['type_name'] == 'Pressure') & (pdata['counterpress'] == True)]
    allpress = pd.concat([press, counterpress], ignore_index=True)

    pitch = VerticalPitch(pitch_type='statsbomb', pitch_color='white', line_color='black', line_zorder=2)
    pitch.draw(ax=ax_pitch)

    # Create custom colormap with three colors
    colors = ['white', 'green']  # Low → High density
    n_bins = 100
    cmap = LinearSegmentedColormap.from_list('custom', colors, N=n_bins)

    # Add heatmap
    if not allpress.empty:
        bin_statistic = pitch.bin_statistic(allpress['x'], allpress['y'], statistic='count', bins=(12, 8))
        pitch.heatmap(bin_statistic, ax=ax_pitch, cmap=cmap, edgecolors='None', alpha=0.75, zorder=1)

    def plot_glow_scatter(pitch, x, y, ax, color, base_size, z, glow_layers=5):
        """Plot scatter with smooth glow effect"""
        for i in range(glow_layers, 0, -1):
            size = base_size * (1 + i * 0.5)
            alpha = 1 / (i + 1)
            pitch.scatter(x, y, ax=ax, color=color, edgecolor='None', 
                        s=size, alpha=alpha, zorder=z + glow_layers - i)

    plot_glow_scatter(pitch, counterpress['x'], counterpress['y'], ax_pitch, '#fb4b44', 35, 15)
    pitch.scatter(counterpress['x'], counterpress['y'], ax=ax_pitch, color='white', edgecolor='None', s=20, alpha=1, zorder=20)

    plot_glow_scatter(pitch, press['x'], press['y'], ax_pitch, "#004cef", 20, 10)
    pitch.scatter(press['x'], press['y'], ax=ax_pitch, color='white', edgecolor='None', s=10, alpha=1, zorder=20)

    # player stats
    ax_text(40, 125.5, f'<Pressing Actions: {len(press)}>   |   <Counter Pressings: {len(counterpress)}>', 
        highlight_textprops=[
            {'color': '#004cef', 'fontproperties': font_regular}, 
            {'color': '#fb4b44', 'fontproperties': font_regular}
        ],
        fontsize=15, ha='center', va='center', ax=ax_pitch, fontproperties=font_regular)
    ax_pitch.set_title(f'Pressing Heatmap', y=1.05, fontproperties=font_bold, fontsize=13)
    ax_pitch.set_xlim(-1, 81)

    # Bar chart for percentiles (possession-adjusted)
    player_rows = pdf[pdf['player_name'] == pname]
    # If a player appears for multiple teams in the season (e.g. mid-season transfer),
    # prefer the row that matches the currently selected/loaded team.
    if selected_team and not player_rows[player_rows['team_name'] == selected_team].empty:
        playerdf = player_rows[player_rows['team_name'] == selected_team].iloc[0]
    else:
        playerdf = player_rows.iloc[0]
    pkname = playerdf['player_known_name']
    if pkname is None:
        pkname = pname
    pltime = playerdf['minutes']
    pteam = playerdf['team_name']
    
    # Use possession-adjusted columns
    stat_columns = ['defensive_actions_90', 'defensive_action_regains_90', 
                    'pressures_90', 'pressure_regains_90', 
                    'counterpressures_90', 'counterpressure_regains_90']
    padj_stat_columns = [f'{col}_padj' for col in stat_columns]
    percentile_cols = [f'{col}_percentile' for col in padj_stat_columns]
    percentiles = [playerdf[col] for col in percentile_cols]
    
    if show_padj_stats_text:
        actual_values = [playerdf[col] for col in padj_stat_columns]
        # Clean labels (add "Poss Adj" to indicate adjustment)
        labels = [col.replace('_90_padj', '( p90)').replace('_', ' ').title() for col in padj_stat_columns]
    else:
        actual_values = [playerdf[col] for col in stat_columns]
        # Clean labels 
        labels = [col.replace('_90', '( p90)').replace('_', ' ').title() for col in stat_columns]
    
    # Create horizontal bar chart
    y_pos = range(len(labels))
    
    # Gray background bars (full width)
    ax_bars.barh(y_pos, [100] * len(labels), color='#808080', height=0.35, alpha=0.3, zorder=1)
    
    # Create colormap for percentiles: Red → Orange → Green
    percentile_colors = ['#fb4b44', '#FFA500', 'green']  # Red → Orange → Green
    percentile_cmap = LinearSegmentedColormap.from_list('percentile', percentile_colors, N=100)
    
    # Actual percentile bars with gradient colors
    for i, (y, val) in enumerate(zip(y_pos, percentiles)):
        # Normalize percentile to 0-1 range for colormap
        norm_value = val / 100.0
        color = percentile_cmap(norm_value)
        ax_bars.barh(y, val, color=color, height=0.35, alpha=0.7, zorder=2)

    # Add scatter points at the end of bars with percentile values
    for i, (y, val) in enumerate(zip(y_pos, percentiles)):
        # Normalize percentile to 0-1 range for colormap
        norm_value = val / 100.0
        color = percentile_cmap(norm_value)
        ax_bars.scatter(-5, i, s=750, color=color, edgecolor='None', linewidth=2, zorder=3)
        # Add percentile value text inside the scatter
        ax_bars.text(-5, i, f'{int(val)}', color='white', va='center', ha='center', fontsize=17, zorder=4, fontproperties=font_regular)
        # ax_bars.text(int(val)+2, i, f'{actual_values[i]:.2f}', color='black', va='center', ha='center', rotation=90, fontsize=10, zorder=4)

    # Add labels at the top of each bar
    for i, (val, label) in enumerate(zip(percentiles, labels)):
        ax_bars.text(0, i-0.35, f'{label}: {actual_values[i]:.2f}', va='center', ha='left', fontsize=15, fontproperties=font_regular)
    
    # Remove all spines and grid
    for spine in ax_bars.spines.values():
        spine.set_visible(False)
    ax_bars.grid(False)
    
    # Remove ticks and labels
    ax_bars.set_yticks([])
    ax_bars.set_xticks([])
    ax_bars.invert_yaxis()
    
    title_suffix = '(Possession Adjusted)' if possession_adjusted else ''
    ax_bars.set_title(f'Stats Percentiles {title_suffix}', pad=0, y=1.065, fontproperties=font_bold, fontsize=13)

    # Calculate overall percentile using weighted average
    overall_percentile = playerdf['overall_percentile']
    overall_norm = overall_percentile / 100.0
    overall_color = percentile_cmap(overall_norm)

    # Plot a big circular scatter at the top right of ax_bars
    # ax_bars.scatter(95, -1.2, s=3000, color=overall_color, edgecolor='None', zorder=10, clip_on=False)
    # ax_bars.text(95, -1.2, f'{int(overall_percentile)}', color='white', va='center', ha='center', fontsize=26, zorder=11, fontproperties=font_con_bold)
    # ax_bars.text(95, -2.1, 'OVERALL\nPERCENTILE', color='#808080', va='center', ha='center', fontsize=10, zorder=11, fontproperties=font_bold)

    try:
        ftmb_tid = df_teamNameId[df_teamNameId['teamName']==pteam].teamId.to_list()[0]
    except Exception:
        ftmb_tid = None

    return pkname, pltime, ftmb_tid, overall_percentile, overall_color

def team_pressing_viz(df, team_name, df_teamNameId, ax_pitch, ax_left=None, ax_bottom=None, ax_right=None, league_csv=None, show_numbers=True):
    # Load fonts
    font_bold = font_manager.FontProperties(fname=os.path.abspath('Boldonse-Regular.ttf'))
    font_regular = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Regular.ttf'))
    font_con_bold = font_manager.FontProperties(fname=os.path.abspath('NotoSans_Condensed-Bold.ttf'))

    pitch_and_label_color = '#e3e3e3'
    pitch = VerticalPitch(pitch_type='statsbomb', pitch_color='white', line_color=pitch_and_label_color, line_zorder=2)
    pitch.draw(ax=ax_pitch)

    # Create custom colormap with two colors
    colors = [
        # Low Color
        # '#22334d',
        # '#ff4b44',
        # '#9CD5FF',
        # '#A50044',
        '#0ac4e0',

        # Mid Color
        '#0992c2',
        # '#7AAACE',
        # '#EDBB00',

        # High Color
        # '#355872',
        '#0b2d72',
        # '#004D98'
        ]  # Low → High density
    n_bins = 100
    cmap = LinearSegmentedColormap.from_list('custom', colors, N=n_bins)

    # Check if we can get full heatmap from CSV
    stat_from_csv = None
    if league_csv:
        try:
            df_league = pd.read_csv(league_csv)
            team_row = df_league[df_league['team_name'] == team_name]
            if not team_row.empty:
                # Attempt to reconstruct 2D bins from "zone_i_j"
                test_col = 'zone_0_0'
                if test_col in df_league.columns:
                    stat_from_csv = np.zeros((5, 6))
                    for i in range(5):
                        for j in range(6):
                            stat_from_csv[i, j] = team_row[f'zone_{i}_{j}'].values[0]
        except Exception as e:
            pass

    # Add heatmap
    if stat_from_csv is not None:
        # Create dummy bin_statistic using just any coordinates to get the dictionary, then replace 'statistic'
        bin_statistic = pitch.bin_statistic([10], [10], statistic='count', bins=(6, 5))
        bin_statistic['statistic'] = stat_from_csv
        pitch.heatmap(bin_statistic, ax=ax_pitch, cmap=cmap, edgecolors='None', alpha=0.75, zorder=1)
        if show_numbers:
            pitch.label_heatmap(bin_statistic, str_format='{:.0f}', color=pitch_and_label_color, ax=ax_pitch, ha='center', va='center', fontproperties=font_con_bold, fontsize=25)
    elif not df.empty:
        bin_statistic = pitch.bin_statistic(df['x'], df['y'], statistic='count', bins=(6, 5))
        pitch.heatmap(bin_statistic, ax=ax_pitch, cmap=cmap, edgecolors='None', alpha=0.75, zorder=1)
        if show_numbers:
            pitch.label_heatmap(bin_statistic, str_format='{:.0f}', color=pitch_and_label_color, ax=ax_pitch, ha='center', va='center', fontproperties=font_con_bold, fontsize=18)

    if not show_numbers and not df.empty:
        pitch.scatter(df['x'], df['y'], ax=ax_pitch, color='white', edgecolor='None', s=2.5, alpha=0.9, zorder=3)

    ax_pitch.set_xlim(-1, 81)
    ax_pitch.set_ylim(-1, 127)
    ax_pitch.axis('off')
    ax_pitch.text(80, 0, '   -----------------', fontsize=20, va='center', color='#808080', fontproperties=font_con_bold)
    ax_pitch.text(80, 40, '   -----------------', fontsize=20, va='center', color='#808080', fontproperties=font_con_bold)
    ax_pitch.text(80, 80, '   -----------------', fontsize=20, va='center', color='#808080', fontproperties=font_con_bold)
    ax_pitch.text(80, 120, '   -----------------', fontsize=20, va='center', color='#808080', fontproperties=font_con_bold)  # Turn off spines for the pitch plot

    # Add 5-rectangle colormap legend at the top right
    x_start = 20
    y_pos = 125
    rect_width = 8
    rect_height = 4
    colors_5 = [cmap(i/4) for i in range(5)]
    for i, c in enumerate(colors_5):
        ax_pitch.bar(x_start + i*rect_width, rect_height, bottom=y_pos, width=rect_width, 
                     color=c, align='edge', alpha=0.75, edgecolor='none', zorder=3)
    ax_pitch.text(x_start - 2, y_pos + rect_height/4, "Less", va='center', ha='right', fontsize=13, fontproperties=font_regular)
    ax_pitch.text(x_start + 5*rect_width + 2, y_pos + rect_height/4, "More", va='center', ha='left', fontsize=13, fontproperties=font_regular)

    if league_csv and ax_left is not None and ax_bottom is not None and ax_right is not None:
        try:
            df_league = pd.read_csv(league_csv)
            team_row = df_league[df_league['team_name'] == team_name]
            
            len_cols = [f'len_{i}' for i in range(6)]
            wid_cols = [f'wid_{i}' for i in range(5)]
            
            if not team_row.empty:
                team_len = team_row[len_cols].values[0]
                team_wid = team_row[wid_cols].values[0]
                team_len_pct = team_len / max(1, team_len.sum()) * 100
                team_wid_pct = team_wid / max(1, team_wid.sum()) * 100
            else:
                team_len_pct = np.zeros(6)
                team_wid_pct = np.zeros(5)
                
            league_len_pct = (df_league[len_cols].div(df_league[len_cols].sum(axis=1).replace(0, 1), axis=0) * 100).mean().values
            league_wid_pct = (df_league[wid_cols].div(df_league[wid_cols].sum(axis=1).replace(0, 1), axis=0) * 100).mean().values
            
            diff_len = team_len_pct - league_len_pct
            diff_wid = team_wid_pct - league_wid_pct
            
            y_pts = [10, 30, 50, 70, 90, 110]
            ax_left.plot(diff_len, y_pts, color='#E452FF', marker='o', mfc='white', lw=2)
            ax_left.plot(np.zeros_like(diff_len), y_pts, color='#808080', marker='o', mfc='white', lw=2)
            ax_left.set_ylim(-4, 124)
            max_diff_len = max(abs(diff_len).max(), 5)
            ax_left.set_xlim(-max_diff_len * 1, max_diff_len * 1)
            ax_left.axis('off')
            
            x_pts = [8, 24, 40, 56, 72]
            ax_bottom.plot(x_pts, diff_wid, color='#E452FF', marker='o', mfc='white', lw=2)
            ax_bottom.plot(x_pts, np.zeros_like(diff_wid), color='#808080', marker='o', mfc='white', lw=2)
            ax_bottom.set_xlim(-1, 81)
            max_diff_wid = max(abs(diff_wid).max(), 5)
            ax_bottom.set_ylim(-max_diff_wid * 1, max_diff_wid * 1)
            ax_bottom.axis('off')
            
            # --- ax_right Text Boxes ---
            df_league['lb_count'] = df_league['len_0'] + df_league['len_1']
            df_league['mb_count'] = df_league['len_2'] + df_league['len_3']
            df_league['ft_count'] = df_league['len_4'] + df_league['len_5']
            
            N_teams = len(df_league)
            df_league['lb_rank'] = df_league['lb_count'].rank(ascending=False, method='min').astype(int)
            df_league['mb_rank'] = df_league['mb_count'].rank(ascending=False, method='min').astype(int)
            df_league['ft_rank'] = df_league['ft_count'].rank(ascending=False, method='min').astype(int)
            
            t_stats = df_league[df_league['team_name'] == team_name]
            if not t_stats.empty:
                t_stats = t_stats.iloc[0]
                total = t_stats['total_pressures']
                lb = t_stats['lb_count']
                mb = t_stats['mb_count']
                ft = t_stats['ft_count']
                
                lb_pct = lb / total * 100 if total > 0 else 0
                mb_pct = mb / total * 100 if total > 0 else 0
                ft_pct = ft / total * 100 if total > 0 else 0
                
                lb_rank = t_stats['lb_rank']
                mb_rank = t_stats['mb_rank']
                ft_rank = t_stats['ft_rank']
                
                def get_color(rank, n_teams):
                    norm = 1 - (rank - 1) / max(1, (n_teams - 1))
                    return cmap(norm)
                
                ax_right.set_ylim(-1, 127)
                ax_right.set_xlim(0, 0.4)
                ax_right.axis('off')
                
                boxes = [
                    (100, ft, ft_pct, ft_rank, 'High Press'),
                    (60, mb, mb_pct, mb_rank, 'Mid-Block'),
                    (20, lb, lb_pct, lb_rank, 'Low-Block')
                ]
                
                for y_center, count, pct, rank, title in boxes:
                    # color = get_color(rank, N_teams)
                    bbox_props = dict(boxstyle='round,pad=1.0', facecolor='white', edgecolor='none', alpha=0.85)
                    text_str = f"{title}:  {pct:.1f}%\n League Rank: {rank}"
                    ax_right.text(0.1, y_center, text_str, ha='left', va='center', 
                                  bbox=bbox_props, color='#808080', 
                                  rotation=90,
                                  fontsize=20, fontproperties=font_con_bold)
                                  
        except Exception as e:
            print("Failed to plot line chart:", e)

    try:
        ftmb_tid = df_teamNameId[df_teamNameId['teamName']==team_name].teamId.to_list()[0]
    except Exception:
        ftmb_tid = None

    return ftmb_tid

def save_league_zonewise_stats(df, directory, league_name, season_name):
    pitch = VerticalPitch(pitch_type='statsbomb', pitch_color='white', line_zorder=2)
    teams = df['team_name'].dropna().unique()
    data = []
    
    for team in teams:
        df_team = df[df['team_name'] == team]
        if not df_team.empty:
            bs = pitch.bin_statistic(df_team['x'], df_team['y'], statistic='count', bins=(6, 5))
            stat = bs['statistic']
            
            row = {'team_name': team, 'total_pressures': len(df_team)}
            len_dist = stat.sum(axis=0)
            wid_dist = stat.sum(axis=1)
            
            for i in range(6):
                row[f'len_{i}'] = len_dist[i]
            for j in range(5):
                row[f'wid_{j}'] = wid_dist[j]
            
            # Save the full 2D array for the heatmap
            for j in range(5):
                for i in range(6):
                    row[f'zone_{j}_{i}'] = stat[j, i]
                    
            data.append(row)
            
    df_out = pd.DataFrame(data)
    league_clean = str(league_name).replace(" ", "_").replace("/", "_")
    season_clean = str(season_name).replace(" ", "_").replace("/", "_")
    csv_filename = os.path.join(directory, f"{league_clean}_{season_clean}_zonewise_pressing.csv")
    df_out.to_csv(csv_filename, index=False)
    return csv_filename
