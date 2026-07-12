//! Watchdog integration for split-brain prevention.
//!
//! On Linux, writes to /dev/watchdog keep the hardware watchdog alive.
//! If the HA loop hangs and can't pet the watchdog within its timeout,
//! the kernel reboots the machine — preventing a zombie primary.
//!
//! On non-Linux or when disabled, this is a no-op.

use crate::config::{WatchdogConfig, WatchdogMode};
#[allow(unused_imports)]
use tracing::{debug, error, info, warn};

/// Watchdog device handler
pub struct Watchdog {
    config: WatchdogConfig,
    /// File descriptor for the watchdog device (Linux only)
    #[cfg(target_os = "linux")]
    fd: Option<std::os::unix::io::RawFd>,
    /// Track whether we've successfully opened the device
    is_active: bool,
}

impl Watchdog {
    /// Create a new Watchdog handler based on configuration
    pub fn new(config: WatchdogConfig) -> Self {
        Self {
            config,
            #[cfg(target_os = "linux")]
            fd: None,
            is_active: false,
        }
    }

    /// Activate the watchdog (open the device).
    /// Called when this node becomes the primary.
    pub fn activate(&mut self) -> bool {
        if self.config.mode == WatchdogMode::Off {
            debug!("Watchdog is disabled");
            return true;
        }

        #[cfg(target_os = "linux")]
        {
            self.activate_linux()
        }

        #[cfg(not(target_os = "linux"))]
        {
            if self.config.mode == WatchdogMode::Required {
                error!("Watchdog required but not available on this platform");
                return false;
            }
            info!("Watchdog not available on this platform (non-Linux)");
            true
        }
    }

    /// Pet (keepalive) the watchdog. Must be called every HA loop cycle
    /// while this node is the primary.
    pub fn keepalive(&self) {
        if !self.is_active {}

        #[cfg(target_os = "linux")]
        {
            self.keepalive_linux();
        }
    }

    /// Safely disarm the watchdog (close without triggering reboot).
    /// Called when this node is demoted or shutting down.
    pub fn disarm(&mut self) {
        if !self.is_active {
            return;
        }

        #[cfg(target_os = "linux")]
        {
            self.disarm_linux();
        }

        self.is_active = false;
        info!("Watchdog disarmed");
    }

    /// Whether the watchdog is currently active (opened and petting)
    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Whether the watchdog is required by configuration
    pub fn is_required(&self) -> bool {
        self.config.mode == WatchdogMode::Required
    }

    /// Get the safety margin in seconds
    pub fn safety_margin(&self) -> u64 {
        self.config.safety_margin
    }

    // ─────────── Linux implementation ───────────

    #[cfg(target_os = "linux")]
    fn activate_linux(&mut self) -> bool {
        use std::ffi::CString;
        use std::os::unix::io::RawFd;

        let path = CString::new(self.config.device.as_str()).unwrap();
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY) };

        if fd < 0 {
            let err = std::io::Error::last_os_error();
            if self.config.mode == WatchdogMode::Required {
                error!(device = %self.config.device, "Cannot open watchdog device: {err}");
                return false;
            } else {
                warn!(device = %self.config.device, "Cannot open watchdog device: {err}");
                return true;
            }
        }

        // Set watchdog timeout via ioctl
        // WDIOC_SETTIMEOUT = 0xC0045706
        let timeout: libc::c_int = 30; // Will be overridden by safety calculation
        unsafe {
            libc::ioctl(fd, 0xC004_5706, &timeout);
        }

        self.fd = Some(fd);
        self.is_active = true;
        info!(device = %self.config.device, "Watchdog activated");
        true
    }

    #[cfg(target_os = "linux")]
    fn keepalive_linux(&self) {
        if let Some(fd) = self.fd {
            let byte: [u8; 1] = [b'1'];
            let ret = unsafe { libc::write(fd, byte.as_ptr() as *const libc::c_void, 1) };
            if ret < 0 {
                warn!("Watchdog keepalive write failed");
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn disarm_linux(&mut self) {
        if let Some(fd) = self.fd.take() {
            // Write magic 'V' character to safely close without triggering reboot
            let magic: [u8; 1] = [b'V'];
            unsafe {
                libc::write(fd, magic.as_ptr() as *const libc::c_void, 1);
                libc::close(fd);
            }
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.disarm();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watchdog_off_mode() {
        let config = WatchdogConfig {
            mode: WatchdogMode::Off,
            device: "/dev/watchdog".to_string(),
            safety_margin: 5,
        };
        let mut wd = Watchdog::new(config);
        assert!(wd.activate()); // always succeeds when off
        assert!(!wd.is_active());
        assert!(!wd.is_required());
    }

    #[test]
    fn test_watchdog_automatic_on_non_linux() {
        let config = WatchdogConfig {
            mode: WatchdogMode::Automatic,
            device: "/dev/watchdog".to_string(),
            safety_margin: 5,
        };
        let mut wd = Watchdog::new(config);
        // On non-Linux (macOS in test), activate succeeds but is_active is false
        assert!(wd.activate());
        assert!(!wd.is_active());
    }

    #[test]
    fn test_watchdog_required_on_non_linux() {
        let config = WatchdogConfig {
            mode: WatchdogMode::Required,
            device: "/dev/watchdog".to_string(),
            safety_margin: 5,
        };
        let mut wd = Watchdog::new(config);
        // On non-Linux, required mode fails
        assert!(!wd.activate());
        assert!(wd.is_required());
    }

    #[test]
    fn test_keepalive_noop_when_inactive() {
        let config = WatchdogConfig {
            mode: WatchdogMode::Off,
            device: "/dev/watchdog".to_string(),
            safety_margin: 5,
        };
        let wd = Watchdog::new(config);
        wd.keepalive(); // Should not panic
    }

    #[test]
    fn test_disarm_noop_when_inactive() {
        let config = WatchdogConfig {
            mode: WatchdogMode::Off,
            device: "/dev/watchdog".to_string(),
            safety_margin: 5,
        };
        let mut wd = Watchdog::new(config);
        wd.disarm(); // Should not panic
    }
}
