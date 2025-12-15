# 🏎️ LMU Telemetry → MoTeC Converter

A community tool to convert **Le Mans Ultimate (LMU)** telemetry stored in **DuckDB** format into **MoTeC i2 (.ld)** logs.

The goal of this project is to provide a **clean, readable and analysis-ready MoTeC log** starting from LMU telemetry, without broken timelines or manual post-processing.

Designed for **sim racers**, **leagues**, **endurance racing**, and **setup analysis**.

---

## ✨ Features

- ✅ Direct import from LMU `.duckdb` telemetry files
- ✅ **Single unified MoTeC log** output (`*_CUSTOM.ld`)
- ✅ Logical channel groups selectable from GUI:
  - Driver / Inputs
  - Powertrain
  - Vehicle Dynamics
  - Aero & Suspension
  - Tyres
  - Track & Environment
  - States & Flags
- ✅ Configurable sampling frequency per group
- ✅ Correct master timeline (no broken graphs)
- ✅ Professional channel naming:
  - Wheels: **FL / FR / RL / RR**
  - Sides: **_L / _R**
  - Tyre layers: **_I / _M / _O**
- ✅ Consistent units:
  - Temperatures: **°C**
  - Pressures: **bar**
  - Heights / suspension: **mm**
  - Speed: **km/h**
  - Engine speed: **rpm**
- ✅ Simple **one-click GUI**

---

## 🖥️ Requirements

- **Windows**
- **Python 3.10+**
- Python packages:
  ```bash
  pip install duckdb pandas numpy
Tkinter is included with the official Windows Python distribution.

🚀 Quick Start
Clone or download this repository

Make sure Python is available in your PATH

Launch the GUI with:

Copia codice
oneclick.bat
Select an LMU .duckdb telemetry file

Choose channel groups and sampling frequencies

Click RUN

📂 Output files will be created in:

php-template
Copia codice
Telemetry/
  <SessionName>_CUSTOM.ld
  <SessionName>_CUSTOM.csv
  <SessionName>_CUSTOM.meta.csv
Open the .ld file directly in MoTeC i2.

📊 MoTeC Output
Single, coherent telemetry log

Channels already renamed and grouped

Beacon and LapTime generated automatically

Ready for overlays, histograms and math channels

⚠️ Disclaimer
This project is not affiliated with:

Studio 397

Motorsport Games

MoTeC Pty Ltd

This is a community-driven, unofficial tool.

📄 License
This project is released under a Non-Commercial License.

✔ Personal use
✔ Educational use
✔ Community / sim-racing use

❌ Commercial use is NOT permitted
❌ Selling, SaaS usage, or integration into commercial products is prohibited without explicit permission from the author

See the LICENSE file for full details.

🤝 Contributing
Pull Requests and improvements are welcome, as long as they remain consistent with the non-commercial nature of the project.

If you wish to use this tool in a commercial context, please contact the author.

🏁 Roadmap (Ideas)
GUI profile presets (Qualifying / Race / Endurance)

Save/load GUI profiles (JSON)

Improved unit detection from metadata

Standalone .exe build

Support for other DuckDB-based simulators

❤️ Credits
LMU sim racing community

MoTeC for the analysis software

Everyone who tests and provides feedback

chatgpt for the help 
