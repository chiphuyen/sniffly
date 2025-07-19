// Project detector for handling project-specific URLs
(function() {
  'use strict';
    
  // Extract project or rollup from URL if present
  window.detectProjectFromURL = function() {
    const path = window.location.pathname;
    const projectMatch = path.match(/^\/project\/(.+)$/);
    const rollupMatch = path.match(/^\/rollup\/(.+)$/);
        
    if (projectMatch) {
      return { type: 'project', name: projectMatch[1] };
    }
    
    if (rollupMatch) {
      return { type: 'rollup', name: rollupMatch[1] };
    }
        
    return null;
  };
    
  // Set project or rollup based on URL detection
  window.setProjectFromURL = async function() {
    const detected = detectProjectFromURL();
        
    if (!detected) {
      return false;
    }
        
    try {
      if (detected.type === 'project') {
        // Call the API to set the project
        const response = await fetch('/api/project-by-dir', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ dir_name: detected.name })
        });
              
        if (!response.ok) {
          const error = await response.json();
          console.error('[ProjectDetector] Failed to set project:', error);
          showError(`Failed to load project: ${error.detail || 'Unknown error'}`);
          return false;
        }
              
        const data = await response.json();
              
        // Update the UI to show the project name
        const projectInfo = document.getElementById('project-info-text');
        if (projectInfo) {
          projectInfo.textContent = `Project: ${data.log_dir_name.replace(/-/g, '/')}`;
        }
        
        // Store project info globally
        window.currentProject = { type: 'project', dirName: detected.name };
        
      } else if (detected.type === 'rollup') {
        // For rollups, we don't need to "set" anything in the server
        // Just store the rollup info globally and update UI
        const projectInfo = document.getElementById('project-info-text');
        if (projectInfo) {
          projectInfo.textContent = `Rollup: ${detected.name}`;
        }
        
        // Store rollup info globally
        window.currentProject = { type: 'rollup', name: detected.name };
      }
            
      return true;
    } catch (error) {
      console.error('[ProjectDetector] Error setting project:', error);
      showError('Failed to load project');
      return false;
    }
  };
    
  // Show error message
  function showError(message) {
    const statsGrid = document.getElementById('overview-stats');
    if (statsGrid) {
      statsGrid.innerHTML = `
                <div style="grid-column: 1 / -1; text-align: center; padding: 2rem; color: #d32f2f;">
                    <h2>Error</h2>
                    <p>${message}</p>
                    <a href="/" style="color: #667eea;">Go to Overview</a>
                </div>
            `;
    }
  }
    
  // Add navigation helper
  window.navigateToOverview = function() {
    window.location.href = '/';
  };
    
  // populateProjectSelector is now handled by the main dashboard code
  // We just need a helper to navigate using project URLs
    
  // Navigate to a project-specific URL
  window.navigateToProject = function(dirName) {
    if (dirName) {
      window.location.href = `/project/${dirName}`;
    }
  };
})();