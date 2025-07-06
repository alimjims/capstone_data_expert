import subprocess
import time
import signal
import sys
import os
import threading
from datetime import datetime

class StockDataOrchestrator:
    def __init__(self):
        self.processes = []
        self.running = False
        
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[ORCHESTRATOR {timestamp}] {message}")
    
    def stream_output(self, process, name, stream_type):
        """Stream output from subprocess in real-time"""
        stream = process.stdout if stream_type == 'stdout' else process.stderr
        
        try:
            for line in iter(stream.readline, ''):
                if line:
                    # Print with process name prefix, handle encoding issues
                    safe_line = line.rstrip().encode('ascii', errors='replace').decode('ascii')
                    print(f"[{name}] {safe_line}")
                if process.poll() is not None:
                    break
        except Exception as e:
            self.log(f"Error streaming {stream_type} for {name}: {e}")
        finally:
            stream.close()
    
    def start_process(self, script_path, description):
        """Start a Python script as a subprocess with real-time output"""
        try:
            self.log(f"Starting {description}...")
            
            # Set environment variable for UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            
            # Start process
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
                env=env  # Use UTF-8 environment
            )
            
            # Start threads to stream output in real-time
            stdout_thread = threading.Thread(
                target=self.stream_output,
                args=(process, description, 'stdout'),
                daemon=True
            )
            stderr_thread = threading.Thread(
                target=self.stream_output,
                args=(process, description, 'stderr'),
                daemon=True
            )
            
            stdout_thread.start()
            stderr_thread.start()
            
            self.processes.append({
                'process': process,
                'name': script_path,
                'description': description,
                'stdout_thread': stdout_thread,
                'stderr_thread': stderr_thread
            })
            
            self.log(f"{description} started with PID: {process.pid}")
            return process
            
        except Exception as e:
            self.log(f"Failed to start {description}: {e}")
            return None
    
    def check_processes(self):
        """Check if all processes are still running"""
        for proc_info in self.processes:
            process = proc_info['process']
            if process.poll() is not None:
                # Process has terminated
                self.log(f"{proc_info['description']} has stopped unexpectedly (exit code: {process.returncode})")
                return False
        return True
    
    def stop_all_processes(self):
        """Gracefully stop all processes"""
        self.log("Stopping all processes...")
        for proc_info in self.processes:
            process = proc_info['process']
            if process.poll() is None:  # Still running
                self.log(f"Stopping {proc_info['description']}...")
                
                # Send SIGTERM first (graceful)
                process.terminate()
                
                # Wait for graceful shutdown
                try:
                    process.wait(timeout=10)
                    self.log(f"{proc_info['description']} stopped gracefully")
                except subprocess.TimeoutExpired:
                    self.log(f"Force killing {proc_info['description']}...")
                    process.kill()
                    process.wait()
        
        self.processes.clear()
        self.log("All processes stopped")
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        self.log("Received shutdown signal")
        self.running = False
        self.stop_all_processes()
        sys.exit(0)
    
    def get_script_paths(self):
        """Get the correct paths to the scripts"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # If running from src/orchestrator directory
        if 'src' in current_dir and 'orchestrator' in current_dir:
            project_root = os.path.dirname(os.path.dirname(current_dir))
        else:
            # If running from project root
            project_root = current_dir
        
        # Define script paths
        generator_script = os.path.join(project_root, "src", "producer", "kafka_dummy_data_generation.py")
        consumer_script = os.path.join(project_root, "src", "consumer", "time_series_db_consumer.py")
        
        # Fallback to check if files exist in different locations
        if not os.path.exists(generator_script):
            # Try alternative names
            alt_generator = os.path.join(project_root, "src", "producer", "fake_stock_generator.py")
            if os.path.exists(alt_generator):
                generator_script = alt_generator
        
        if not os.path.exists(consumer_script):
            # Try alternative names
            alt_consumer = os.path.join(project_root, "src", "consumer", "kafka_consumer.py")
            if os.path.exists(alt_consumer):
                consumer_script = alt_consumer
            
            # Try clean consumer
            alt_consumer2 = os.path.join(project_root, "clean_kafka_consumer.py")
            if os.path.exists(alt_consumer2):
                consumer_script = alt_consumer2
                
            # Try improved consumer
            alt_consumer3 = os.path.join(project_root, "improved_kafka_consumer.py")
            if os.path.exists(alt_consumer3):
                consumer_script = alt_consumer3
        
        return generator_script, consumer_script
    
    def run(self):
        """Main orchestration loop"""
        # Set up signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.log("Starting Stock Data Pipeline Orchestrator")
        self.log("=" * 50)
        
        # Get script paths
        generator_script, consumer_script = self.get_script_paths()
        
        self.log(f"Looking for generator at: {generator_script}")
        self.log(f"Looking for consumer at: {consumer_script}")
        
        # Check if script files exist
        if not os.path.exists(generator_script):
            self.log(f"Generator script not found!")
            self.log(f"Current directory: {os.getcwd()}")
            self.log(f"Looking in: {os.path.dirname(generator_script)}")
            if os.path.exists(os.path.dirname(generator_script)):
                files = os.listdir(os.path.dirname(generator_script))
                self.log(f"Files in producer directory: {files}")
            return
        
        if not os.path.exists(consumer_script):
            self.log(f"Consumer script not found!")
            self.log(f"Looking in: {os.path.dirname(consumer_script)}")
            if os.path.exists(os.path.dirname(consumer_script)):
                files = os.listdir(os.path.dirname(consumer_script))
                self.log(f"Files in consumer directory: {files}")
            return
        
        self.log("Both scripts found! Starting processes...")
        
        # Start the generator first
        generator_process = self.start_process(generator_script, "GENERATOR")
        if not generator_process:
            return
        
        # Wait a moment for generator to initialize
        self.log("Waiting for generator to initialize...")
        time.sleep(5)
        
        # Start the consumer
        consumer_process = self.start_process(consumer_script, "CONSUMER")
        if not consumer_process:
            self.stop_all_processes()
            return
        
        self.log("Both processes started successfully!")
        self.log("You should see output from both processes below.")
        self.log("Press Ctrl+C to stop both processes")
        self.log("=" * 50)
        
        # Monitor processes
        self.running = True
        check_interval = 30  # Check every 30 seconds
        
        while self.running:
            try:
                if not self.check_processes():
                    self.log("One or more processes failed, stopping all...")
                    break
                
                # Show status less frequently to avoid spam
                if int(time.time()) % 60 == 0:
                    self.log(f"Status: {len(self.processes)} processes running")
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                break
        
        self.stop_all_processes()

def main():
    orchestrator = StockDataOrchestrator()
    orchestrator.run()

if __name__ == "__main__":
    main()