import os
import sys
import threading
import time
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox

APP_TITLE = "DuckDB → MoTeC (CUSTOM – gruppi logici)"


class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tip_window = None
        self.widget.bind("<Enter>", self.show)
        self.widget.bind("<Leave>", self.hide)

    def show(self, _event=None):
        if self.tip_window is not None:
            return
        bbox = self.widget.bbox("insert")
        cy = bbox[3] if bbox else 0
        x = self.widget.winfo_rootx() + 25
        y = self.widget.winfo_rooty() + cy + 25
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            justify=tk.LEFT,
            background="#ffffe0",
            relief=tk.SOLID,
            borderwidth=1,
            font=("tahoma", "8", "normal"),
            padx=6,
            pady=3,
            wraplength=320,
        )
        label.pack(ipadx=1)

    def hide(self, _event=None):
        tw = self.tip_window
        if tw is not None:
            tw.destroy()
        self.tip_window = None

GROUPS = {
    "Driver": 100,
    "Powertrain": 100,
    "Dynamics": 100,
    "AeroSusp": 50,
    "Tyres": 20,
    "Environment": 10,
    "States": 20
}

GROUP_DESCRIPTIONS = {
    "Driver": "Driver inputs (brake, throttle, steering)",
    "Powertrain": "Engine and drivetrain telemetry (rpm, gear, torque)",
    "Dynamics": "Vehicle motion data (speed, acceleration, yaw)",
    "AeroSusp": "Aero and suspension metrics (ride height, damping)",
    "Tyres": "Tyre state and temps/pressures",
    "Environment": "Ambient and track conditions",
    "States": "Session/vehicle state flags"
}

def run_chain(cmds, log_widget, cwd=None, progress_cb=None):
    def notify(event):
        if progress_cb:
            log_widget.after(0, progress_cb, event)

    def worker():
        total = len(cmds)
        stopped = False
        for idx, cmd in enumerate(cmds, 1):
            log_widget.insert(tk.END, "\n$ " + " ".join(cmd) + "\n")
            log_widget.see(tk.END)
            start_time = time.time()
            notify({
                "type": "start",
                "index": idx,
                "total": total,
                "cmd": cmd,
                "start": start_time,
            })
            try:
                p = subprocess.Popen(
                    cmd,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                for line in p.stdout:
                    log_widget.insert(tk.END, line)
                    log_widget.see(tk.END)
                    notify({
                        "type": "tick",
                        "index": idx,
                        "total": total,
                        "elapsed": time.time() - start_time,
                    })
                code = p.wait()
                notify({
                    "type": "end",
                    "index": idx,
                    "total": total,
                    "elapsed": time.time() - start_time,
                    "returncode": code,
                })
                log_widget.insert(tk.END, f"\n[exit code: {code}]\n")
                log_widget.see(tk.END)
                if code != 0:
                    log_widget.insert(tk.END, "\nSTOP: comando fallito.\n")
                    log_widget.see(tk.END)
                    notify({
                        "type": "stop",
                        "index": idx,
                        "total": total,
                        "elapsed": time.time() - start_time,
                        "returncode": code,
                    })
                    stopped = True
                    break
                if idx < total:
                    pause_msg = "Attendere: preparazione del prossimo passaggio..."
                    log_widget.insert(tk.END, f"\n{pause_msg}\n")
                    log_widget.see(tk.END)
                    notify({
                        "type": "between",
                        "index": idx,
                        "total": total,
                        "message": pause_msg,
                    })
            except Exception as e:
                log_widget.insert(tk.END, f"\n[ERROR] {e}\n")
                log_widget.see(tk.END)
                notify({
                    "type": "error",
                    "index": idx,
                    "total": total,
                    "elapsed": time.time() - start_time,
                    "error": str(e),
                })
                stopped = True
                break
        notify({"type": "done", "stopped": stopped})
    threading.Thread(target=worker, daemon=True).start()

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("900x620")

        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = tk.StringVar(value="")

        self.group_vars = {}
        self.hz_vars = {}
        self.status_var = tk.StringVar(value="Pronto")
        self.percent_var = tk.StringVar(value="0%")
        self.elapsed_var = tk.StringVar(value="0.0s")

        top = tk.Frame(self)
        top.pack(fill=tk.X, padx=10, pady=8)

        tk.Label(top, text="DuckDB (.duckdb):").grid(row=0, column=0, sticky="w")
        tk.Entry(top, textvariable=self.db_path, width=80).grid(row=0, column=1, sticky="we", padx=6)
        tk.Button(top, text="Scegli...", command=self.pick_db).grid(row=0, column=2)
        top.grid_columnconfigure(1, weight=1)

        grp = tk.LabelFrame(self, text="Gruppi logici")
        grp.pack(fill=tk.X, padx=10, pady=8)

        row = 0
        for g, hz in GROUPS.items():
            v = tk.BooleanVar(value=True)
            h = tk.StringVar(value=str(hz))
            self.group_vars[g] = v
            self.hz_vars[g] = h

            chk = tk.Checkbutton(grp, text=g, variable=v)
            chk.grid(row=row, column=0, sticky="w")
            desc = GROUP_DESCRIPTIONS.get(g, "")
            if desc:
                ToolTip(chk, desc)
            tk.Label(grp, text="Hz").grid(row=row, column=1)
            tk.Entry(grp, textvariable=h, width=6).grid(row=row, column=2, padx=4)
            row += 1

        tk.Button(
            self,
            text="ESEGUI → CUSTOM MoTeC",
            command=self.run_all,
            height=2
        ).pack(pady=10)

        status = tk.Frame(self)
        status.pack(fill=tk.X, padx=10)

        tk.Label(status, text="Stato:").grid(row=0, column=0, sticky="w")
        tk.Label(status, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(4, 20))

        tk.Label(status, text="Percentuale totale:").grid(row=0, column=2, sticky="w")
        tk.Label(status, textvariable=self.percent_var).grid(row=0, column=3, sticky="w", padx=(4, 20))

        tk.Label(status, text="Tempo passo:").grid(row=0, column=4, sticky="w")
        tk.Label(status, textvariable=self.elapsed_var).grid(row=0, column=5, sticky="w")

        logf = tk.LabelFrame(self, text="Log")
        logf.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)

        self.log = tk.Text(logf, wrap="word")
        self.log.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)

        sb = tk.Scrollbar(logf, command=self.log.yview)
        sb.pack(fill=tk.Y, side=tk.RIGHT)
        self.log.config(yscrollcommand=sb.set)

    def pick_db(self):
        path = filedialog.askopenfilename(
            title="Seleziona DuckDB",
            filetypes=[("DuckDB", "*.duckdb"), ("All files", "*.*")]
        )
        if path:
            self.db_path.set(path)

    def run_all(self):
        db = self.db_path.get().strip()
        if not db or not os.path.exists(db):
            messagebox.showerror("Errore", "Seleziona un file .duckdb valido.")
            return

        selected = {}
        for g, v in self.group_vars.items():
            if v.get():
                try:
                    hz = int(self.hz_vars[g].get())
                    if hz <= 0:
                        raise ValueError
                    selected[g] = hz
                except Exception:
                    messagebox.showerror("Errore", f"Hz non valido per gruppo {g}")
                    return

        if not selected:
            messagebox.showerror("Errore", "Seleziona almeno un gruppo.")
            return

        master_hz = max(selected.values())

        out_dir = os.path.join(self.project_dir, "Telemetry")
        os.makedirs(out_dir, exist_ok=True)

        base = os.path.splitext(os.path.basename(db))[0]
        csv_out = os.path.join(out_dir, f"{base}_CUSTOM.csv")
        ld_out = os.path.join(out_dir, f"{base}_CUSTOM")

        unified = os.path.join(self.project_dir, "duckdb_to_motec_unified.py")
        generator = os.path.join(self.project_dir, "motec_log_generator.py")

        args = [f"{g}={hz}" for g, hz in selected.items()]

        cmds = [
            [sys.executable, unified, db, csv_out, *args],
            [sys.executable, generator, csv_out, "CSV", "--frequency", str(master_hz), "--output", ld_out]
        ]

        self.log.insert(tk.END, f"\nOutput in: {out_dir}\n")
        self.log.see(tk.END)

        self.status_var.set("In esecuzione...")
        self.percent_var.set("0%")
        self.elapsed_var.set("0.0s")

        def handle_progress(event):
            etype = event.get("type")
            total = event.get("total", 1)
            index = event.get("index", 0)

            if etype in {"start", "tick", "end", "stop", "error"}:
                elapsed = event.get("elapsed", 0.0)
                self.elapsed_var.set(f"{elapsed:.1f}s")

                completed = index - 1 if etype == "start" else index
                percent = int((completed / total) * 100)
                self.percent_var.set(f"{percent}%")
                self.status_var.set(f"Passo {index}/{total}")

            if etype == "between":
                msg = event.get("message", "")
                self.status_var.set(msg or "In attesa del prossimo passaggio…")

            if etype == "done":
                if event.get("stopped"):
                    self.status_var.set("Interrotto")
                else:
                    self.status_var.set("Completato")
                    self.percent_var.set("100%")
                self.elapsed_var.set("-")

            if etype in {"stop", "error"}:
                self.status_var.set("Interrotto")

        run_chain(cmds, self.log, cwd=self.project_dir, progress_cb=handle_progress)

if __name__ == "__main__":
    App().mainloop()
