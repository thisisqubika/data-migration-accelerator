def get_page_config_styles():
    """Get CSS styles for page configuration."""
    return """
    <style>
    .main .block-container {
        padding-top: 1rem;
        max-width: 100%;
    }

    /* Keep sidebar always visible and prevent collapse */
    [data-testid="stSidebar"] {
        display: block !important;
        visibility: visible !important;
        opacity: 1 !important;
        width: 21rem !important;
        min-width: 21rem !important;
        transform: none !important;
        transition: none !important;
    }

    /* Force sidebar to stay expanded */
    [data-testid="stSidebar"][data-collapsed="true"] {
        width: 21rem !important;
        min-width: 21rem !important;
        transform: translateX(0) !important;
    }

    /* Hide collapse buttons but keep a restore option */
    .css-1lcbmhc { /* Sidebar collapse button */
        display: none !important;
    }

    /* Show hamburger menu button as fallback to restore sidebar */
    button[kind="header"] {
        display: block !important;
        position: fixed !important;
        top: 1rem !important;
        left: 1rem !important;
        z-index: 999999 !important;
        background: #ff4b4b !important;
        color: white !important;
        border: none !important;
        border-radius: 4px !important;
        padding: 0.5rem !important;
    }

    /* Hide Streamlit branding */
    .stDeployButton {{ display: none; }}
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}
    </style>
    """


def get_header_styles():
    """Get CSS styles for the fancy header."""
    return """
    <style>
    .fancy-header {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        padding: 1rem 0;
        margin-bottom: 1.5rem;
        border-bottom: 2px solid #ff4b4b;
        background: linear-gradient(135deg, #000000 0%, #1a1a1a 50%, #000000 100%);
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 20px rgba(255, 75, 75, 0.3), 0 0 30px rgba(0, 0, 0, 0.8);
        border: 1px solid #333;
    }

    .logo-container {
        margin-right: 1.5rem;
        display: flex;
        align-items: center;
    }

    .logo-container img {
        width: 80px;
        height: 80px;
        object-fit: contain;
        filter: drop-shadow(0 0 15px rgba(255, 75, 75, 0.4))
                drop-shadow(2px 2px 8px rgba(255, 255, 255, 0.1))
                brightness(1.1);
    }

    .title-container {
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #ffffff;
        margin: 0;
        background: linear-gradient(45deg, #ff4b4b, #ff8080, #ffaa80);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        text-shadow: 0 0 20px rgba(255, 75, 75, 0.5);
        filter: drop-shadow(0 0 10px rgba(255, 75, 75, 0.3));
    }

    .subtitle {
        font-size: 1rem;
        color: #cccccc;
        margin: 0.25rem 0 0 0;
        font-weight: 400;
        text-shadow: 0 0 10px rgba(255, 255, 255, 0.2);
    }

    /* Responsive design */
    @media (max-width: 768px) {
        .fancy-header {
            flex-direction: column;
            text-align: center;
        }
        .logo-container {
            margin-right: 0;
            margin-bottom: 1rem;
        }
        .main-title {
            font-size: 1.8rem;
        }
    }

    /* Job status indicators */
    .status-success {
        color: #28a745;
        font-weight: bold;
    }

    .status-failed {
        color: #dc3545;
        font-weight: bold;
    }

    .status-running {
        color: #ffc107;
        font-weight: bold;
    }

    .status-pending {
        color: #6c757d;
        font-weight: bold;
    }

    /* Button styling */
    .stButton button {
        border-radius: 6px !important;
        font-weight: 500 !important;
    }

    .stButton button[data-baseweb="button"] {
        background: linear-gradient(135deg, #ff4b4b, #ff6b6b) !important;
        border: none !important;
        box-shadow: 0 2px 10px rgba(255, 75, 75, 0.3) !important;
    }

    .stButton button[data-baseweb="button"]:hover {
        background: linear-gradient(135deg, #ff6b6b, #ff4b4b) !important;
        box-shadow: 0 4px 15px rgba(255, 75, 75, 0.4) !important;
    }
    </style>
    """