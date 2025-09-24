# Visual Asset Browser (VAB)

Welcome to the Visual Asset Browser! This is a powerful command-line tool designed to help you create a "smart" search engine for your entire DAZ Studio content librar.

Instead of just searching by keywords, VAB lets you search by *meaning* or "vibe." You can ask it for "gritty cyberpunk outfits" or "elegant fantasy scenes" and get relevant results from your own collection.

**(Optional: Insert a cool screenshot of your future React UI here to show them the ultimate goal)**

## Features

*   **Semantic Search:** Find assets by describing what you want, not just by remembering a product name.
*   **Hybrid Filtering:** Combine a "vibe" search with hard filters like "Genesis 9" or a specific artist.
*   **DAZ Store Integration:** Automatically fetches and scrapes rich metadata for your official DAZ products.
*   **3rd-Party Content Indexing:** Scans your local libraries to make content from Renderosity, Patreon, etc., fully searchable.
*   **Scene Analysis:** Analyze a `.duf` scene file to see a complete list of the products you used.
*   **API Server Mode:** Run VAB as a backend server to power a future web-based user interface.

## Getting Started: Installation Guide

Follow these steps to get VAB up and running. This process usually takes 15-30 minutes, with most of that time spent on automatic downloads.

### Step 1: Install Python

This tool is built with Python. If you don't have it, you'll need to install it first. We recommend **Python 3.11**.

1.  Go to the [official Python website's download page for Windows](https://www.python.org/downloads/windows/).
2.  Download the installer for the latest Python 3.11.x version.
3.  Run the installer. **IMPORTANT:** On the first screen of the installer, make sure to check the box at the bottom that says **"Add Python to PATH"**. This is crucial!
4.  Click "Install Now" and follow the prompts.

### Step 2: Download the VAB Project

1.  Go to the [VAB GitHub Repository page](https://github.com/your-username/your-repo-name).
2.  Click the green **`< > Code`** button.
3.  Click **"Download ZIP"**.
4.  Unzip the downloaded file into a permanent location on your computer, for example `C:\Tools\VAB`.

### Step 3: Set Up the "Workshop" (Virtual Environment)

We need to create a clean, isolated space for all of VAB's tools and libraries. This is called a "virtual environment."

1.  Open the Windows Command Prompt or PowerShell. You can do this by opening the Start Menu, typing `cmd` or `powershell`, and pressing Enter.
2.  Navigate to your VAB project folder using the `cd` (change directory) command. For example:
    ```cmd
    cd C:\Tools\VAB
    ```
3.  Now, run this command to create the virtual environment. It will create a new `venv` folder inside your project directory.
    ```cmd
    python -m venv venv
    ```
4.  Next, you need to "activate" this environment. This tells your terminal to use this isolated workshop for all future commands.
    ```cmd
    venv\Scripts\activate
    ```
    You will know it worked because your terminal prompt will change to show `(venv)` at the beginning.

5. Configure the applicaton for your local environment

   ```cmd
   copy .env.example .env
   ```
   You will want to edit the .env file with your favorite editor (like Notepad) and change the two items that are marked "TODO":



### Step 4: Install the VAB Application

Now that your workshop is ready, we can install the base application and its core dependencies.

Run this command (make sure your venv is still active):
```cmd
pip install -e .
```

### Step 5: Install the AI Brains (Local LLM Dependencies)

This is the final installation step. To enable the powerful local AI features, you need to install some larger libraries, including the PyTorch engine. We've created a guided installer to make this easy.

Run the interactive installer script:
```cmd
python vab.py install
```
This script will ask you whether you have an NVIDIA GPU and help you install the correct, high-performance libraries for your system. For most users with modern NVIDIA cards (RTX 20-series or newer), the **CUDA 12.1** option is the best choice. If you don't have an NVIDIA card, choose the **CPU Only** option.

This step will download a lot of data and may take some time.

**Congratulations! VAB is now fully installed.**

---

## How to Use VAB: The Workflow

Using VAB is a step-by-step process to build and search your index. Run all commands from your project folder in a terminal with the `(venv)` activated.

### 1. Build Your Database

First, you need to tell VAB about all your content. Remember that DAZ Studio should be up and running before you start!

*   **Fetch DAZ Content:** Run the `fetch` command. The scripts talks to DAZ Studio about all the products you've installed.
    ```cmd
    python vab.py fetch
    ```
*   **Scrape Details:** Now, run `scrape` to gather the rich descriptions for your official DAZ products. This can take a long time if you have a large library, so please be patient. 
    ```cmd
    python vab.py scrape
    ```
### 2. Create the Search Index

After your database is populated, you need to "load" it into the smart search index. This process generates the AI "embeddings" for each product.

```cmd
python vab.py rebuild
```
This is a one-time process that can be slow, but it's what makes the semantic search possible.

### 3. Search Your Content!

Now for the fun part. Use the `query` command to search your library.

*   **Simple "Vibe" Search:**
    ```cmd
    python vab.py query "gritty cyberpunk street clothes"
    ```

    Produces this output:
    ```
    Starting query command...
    --- Loading embedding model: mixedbread-ai/mxbai-embed-large-v1 ---
    --- Embedding model loaded. ---
    +++ Using mixedbread-ai/mxbai-embed-large-v1 for embedding +++
    ┏━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┓
    ┃ #    ┃ Name                                     ┃ Artist                     ┃ Category        ┃    Score ┃ SKU        ┃
    ┡━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━┩
    │ 1    │ Cyberpunk Street Props                   │ ["Polish"]                 │ Environments    │   0.3038 │ 81460      │
    │ 2    │ Cyberpunk Back Alley                     │ ["Polish"]                 │ Props           │   0.3095 │ 81418      │
    │ 3    │ dForce Night Runner Outfit for Genesis … │ ["Daz Originals", "GolaM"] │ People          │   0.3460 │ 54841      │
    │ 4    │ dForce Urban Fall Style Outfit for Gene… │ ["PandyGirl",              │ N/A             │   0.3466 │ 92088      │
    │      │                                          │ "WildDesigns"]             │                 │          │            │
    │ 5    │ Cyberpunk Dark City                      │ ["Dreamlight", "Reedux     │ Environments    │   0.3476 │ 89352      │
    │      │                                          │ Studio"]                   │                 │          │            │
    └──────┴──────────────────────────────────────────┴────────────────────────────┴─────────────────┴──────────┴────────────┘
    Showing 5 of 45 total matches.
    ```    
*   **Search with Filters:**
    ```cmd
    python vab.py query "elegant gown" --categories Clothing --compatible_figures "Genesis 9"
    ```
    Produces this output:
    ```
    Starting query command...
    --- Loading embedding model: mixedbread-ai/mxbai-embed-large-v1 ---
    --- Embedding model loaded. ---
    +++ Using mixedbread-ai/mxbai-embed-large-v1 for embedding +++
    ┏━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┓
    ┃ #    ┃ Name                                     ┃ Artist                     ┃ Category        ┃    Score ┃ SKU        ┃
    ┡━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━┩
    │ 1    │ dForce Aria Outfit for Genesis 9         │ ["Daz Originals", "Nelmi"] │ Clothes         │   0.2739 │ 90659      │
    │ 2    │ dForce Aquarius Gown for Genesis 3 and … │ ["Sshodan"]                │ Female          │   0.2836 │ 69235      │
    │ 3    │ dForce Gown of Fantasy 2 for Genesis 8 … │ ["outoftouch"]             │ Clothes         │   0.3241 │ 69465      │
    │ 4    │ dForce Rochelle Gown for Genesis 8 Fema… │ ["Daz Originals",          │ People          │   0.3285 │ 56213      │
    │      │                                          │ "PandyGirl"]               │                 │          │            │
    │ 5    │ dForce Aurea Regina Outfit for Genesis … │ ["Daz Originals", "Arki"]  │ Clothes         │   0.3459 │ 83777      │
    └──────┴──────────────────────────────────────────┴────────────────────────────┴─────────────────┴──────────┴────────────┘
    Showing 5 of 45 total matches.
    ```

### Other Useful Commands

*   **`stats`**: See a summary of your indexed content, including a list of the most common tags.
    ```cmd
    python vab.py stats
    ```
*   **`server`**: Run the API server for the upcoming web user interface.
    ```cmd
    python vab.py server
    ```

We hope this tool helps you rediscover the amazing assets in your collection!


### TODO:
- Add support for third party content or content that has been manually installed.
- Wrap in a GUI shell so we don't need to use a command line and we don't have to repeatedly load the embedding model. Makes things go faster!









