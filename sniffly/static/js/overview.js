// Overview page JavaScript
let allProjects = [];
let filteredProjects = [];
let globalStats = null;
let tokenUsageChart = null;
let costTrendChart = null;
let lastChartDataHash = null; // Track last chart data to detect changes

// Date range pickers
let overviewTokenDatePicker = null;
let overviewCostDatePicker = null;

// Store full global stats for date range updates
let fullGlobalStats = null;

// Pagination state
let currentProjectsPage = 1;
let projectsPerPage = 10;
let sortColumn = 'last_modified';
let sortDirection = 'desc';

// Background update state
let updateCheckInterval = null;
let chartUpdateInterval = null;
let projectUpdateMap = new Map(); // Track which projects have been updated
let processedProjects = new Set(); // Track which projects have been fully processed

// Initialize the page
document.addEventListener('DOMContentLoaded', async function() {
  console.log('[Overview] Initializing overview page');
    
  // Show the refresh button
  const refreshButton = document.getElementById('refresh-button');
  if (refreshButton) {
    refreshButton.style.display = 'block';
  }
    
  // Load projects immediately (Phase 1)
  await loadProjects();
    
  // Load global stats in background (Phase 2)
  loadGlobalStats();
    
  // Start background updates for project stats
  startBackgroundUpdates();
    
  // Start background updates for charts
  startChartUpdates();
});

// Start checking for background updates
function startBackgroundUpdates() {
  // Clear any previous processed projects tracking
  processedProjects.clear();
    
  // Count projects without stats
  const projectsWithoutStats = allProjects.filter(p => p.stats === null).length;
    
  if (projectsWithoutStats === 0) {
    console.log('[Overview] All projects already have stats, no background updates needed');
    return;
  }
    
  console.log(`[Overview] Starting background updates for ${projectsWithoutStats} projects without stats`);
    
  // Check every 2 seconds for updates
  updateCheckInterval = setInterval(checkForProjectUpdates, 2000);
    
  // Stop after 60 seconds to avoid indefinite polling
  setTimeout(() => {
    if (updateCheckInterval) {
      clearInterval(updateCheckInterval);
      updateCheckInterval = null;
      const stillWithoutStats = allProjects.filter(p => p.stats === null).length;
      console.log(`[Overview] Stopped checking for background updates. ${stillWithoutStats} projects still without stats.`);
    }
  }, 60000);
}

// Check for project updates
async function checkForProjectUpdates() {
  try {
    console.log('[Overview] Checking for project updates...');
        
    // Only fetch projects data, not full stats
    const response = await fetch('/api/projects?include_stats=true');
    if (!response.ok) {
      console.error('[Overview] Failed to fetch projects:', response.status);
      return;
    }
        
    const data = await response.json();
    const updatedProjects = data.projects || [];
        
    console.log(`[Overview] Received ${updatedProjects.length} projects from API`);
        
        
    let hasUpdates = false;
    let updatedCount = 0;
        
    // Check each project for updates
    for (let i = 0; i < updatedProjects.length; i++) {
      const updatedProject = updatedProjects[i];
            
      // Skip if we've already fully processed this project
      if (processedProjects.has(updatedProject.log_path)) {
        continue;
      }
            
      const existingProjectIndex = allProjects.findIndex(p => p.log_path === updatedProject.log_path);
            
      if (existingProjectIndex !== -1) {
        const existingProject = allProjects[existingProjectIndex];
                
                
        // Verify we're matching the correct projects
        if (existingProject.display_name !== updatedProject.display_name) {
          console.error('[Overview] Project mismatch! Different names:', 
            existingProject.display_name, 'vs', updatedProject.display_name);
        }
                
        // Check if this project now has stats when it didn't before
        const hadStats = existingProject.stats !== null && existingProject.stats !== undefined;
        const hasStats = updatedProject.stats !== null && updatedProject.stats !== undefined;
                
        // Store the actual values before any updates
        const actualOldCost = existingProject.stats?.total_cost || 0;
        const actualNewCost = updatedProject.stats?.total_cost || 0;
                
        if (!hadStats && hasStats) {
          // Project stats became available
          console.log(`[Overview] Stats now available for: ${updatedProject.display_name}`);
          console.log(`[Overview] Cost: ${actualNewCost}`);
                    
          // Replace the entire project object to ensure clean data
          allProjects[existingProjectIndex] = {
            ...existingProject,
            stats: JSON.parse(JSON.stringify(updatedProject.stats)),
            last_modified: updatedProject.last_modified,
            in_cache: updatedProject.in_cache
          };
                    
          // Also update in filtered projects
          const filteredIndex = filteredProjects.findIndex(p => p.log_path === updatedProject.log_path);
          if (filteredIndex !== -1) {
            filteredProjects[filteredIndex] = allProjects[existingProjectIndex];
          }
                    
          hasUpdates = true;
          updatedCount++;
          projectUpdateMap.set(updatedProject.log_path, true);
          processedProjects.add(updatedProject.log_path);
                    
        } else if (hadStats && hasStats && Math.abs(actualOldCost - actualNewCost) > 0.001) {
          // Stats changed
          console.log(`[Overview] Stats updated for: ${updatedProject.display_name} (cost: ${actualOldCost} -> ${actualNewCost})`);
                    
          // Replace the entire project object to ensure clean data
          allProjects[existingProjectIndex] = {
            ...existingProject,
            stats: JSON.parse(JSON.stringify(updatedProject.stats)),
            last_modified: updatedProject.last_modified,
            in_cache: updatedProject.in_cache
          };
                    
          // Also update in filtered projects
          const filteredIndex = filteredProjects.findIndex(p => p.log_path === updatedProject.log_path);
          if (filteredIndex !== -1) {
            filteredProjects[filteredIndex] = allProjects[existingProjectIndex];
          }
                    
          hasUpdates = true;
          updatedCount++;
          projectUpdateMap.set(updatedProject.log_path, true);
          processedProjects.add(updatedProject.log_path);
        } else if (hadStats && hasStats) {
          // Project already has stats and they haven't changed
          // Mark it as fully processed so we don't keep checking it
          processedProjects.add(updatedProject.log_path);
        }
      }
    }
        
    // If we have updates, refresh the current page view
    if (hasUpdates) {
      console.log(`[Overview] Updating table with ${updatedCount} project(s)`);
      updateCurrentPageView();
    }
        
    // If all projects have been processed, stop checking
    if (processedProjects.size >= allProjects.length) {
      clearInterval(updateCheckInterval);
      updateCheckInterval = null;
      console.log('[Overview] All projects have been processed, stopping background updates');
    }
        
  } catch (error) {
    // Log errors for debugging
    if (error.message && error.message.includes('Failed to fetch')) {
      console.warn('[Overview] Server connection lost, stopping background updates');
      clearInterval(updateCheckInterval);
      updateCheckInterval = null;
    } else {
      console.error('[Overview] Background update check failed:', error);
    }
  }
}

// Update only the currently visible page
function updateCurrentPageView() {
  console.log('[Overview] Updating changed rows...');
    
  // Instead of re-rendering the entire table, just update the changed rows
  projectUpdateMap.forEach((wasUpdated, projectPath) => {
    const row = document.querySelector(`tr[data-project-path="${projectPath}"]`);
    if (row) {
      // Find the project data
      const project = allProjects.find(p => p.log_path === projectPath);
      if (project) {
        updateSingleRow(row, project);
      }
    }
  });
    
  // Clear the update map after animation
  setTimeout(() => {
    projectUpdateMap.clear();
  }, 2000);
}

// Update a single row in the table
function updateSingleRow(row, project) {
  const stats = project.stats || {};
  const hasStats = project.stats !== null;
  const lastModified = new Date(project.last_modified * 1000);
    
  // Calculate duration
  let duration = '-';
  if (hasStats && stats.first_message_date && stats.last_message_date) {
    const firstDate = new Date(stats.first_message_date);
    const lastDate = new Date(stats.last_message_date);
    const diffMs = lastDate - firstDate;
    // Add 1 to make it inclusive (e.g., Jun 26 to Jun 28 = 3 days)
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24)) + 1;
        
    if (diffDays === 1) {
      duration = '1 day';
    } else if (diffDays < 30) {
      duration = `${diffDays} days`;
    } else if (diffDays < 365) {
      const months = Math.floor(diffDays / 30);
      duration = months === 1 ? '1 month' : `${months} months`;
    } else {
      const years = Math.floor(diffDays / 365);
      duration = years === 1 ? '1 year' : `${years} years`;
    }
  }
    
  // Format cost
  const cost = hasStats && stats.total_cost ? 
    `$${stats.total_cost.toFixed(2)}` : '-';
  
  // Calculate tokens per command
  let tokensPerCmd = '-';
  if (hasStats && stats.avg_tokens_per_command !== undefined) {
    tokensPerCmd = stats.avg_tokens_per_command.toFixed(1);
  }
  
  // Calculate steps per command
  let stepsPerCmd = '-';
  if (hasStats && stats.avg_steps_per_command !== undefined) {
    stepsPerCmd = stats.avg_steps_per_command.toFixed(1);
  }
  
  // Calculate commands per context
  let cmdsPerContext = '-';
  if (hasStats && stats.compact_summary_count > 0 && stats.total_commands > 0) {
    const contextRatio = Math.floor(stats.total_commands / stats.compact_summary_count);
    cmdsPerContext = contextRatio.toString();
  }
  
  // Calculate books
  let books = '-';
  if (hasStats && stats.total_commands && stats.avg_tokens_per_command) {
    const totalWords = stats.total_commands * stats.avg_tokens_per_command * 3 / 4;
    const bookCount = totalWords / 60000;
    books = bookCount >= 1 ? bookCount.toFixed(1) : bookCount.toFixed(2);
  }
    
  // Update row cells
  const cells = row.querySelectorAll('td');
  cells[1].textContent = formatDate(lastModified.toISOString());
  cells[2].textContent = duration;
  cells[3].textContent = cost;
  cells[4].textContent = hasStats ? formatNumber(stats.total_commands || 0) : '-';
  cells[5].textContent = tokensPerCmd;
  cells[6].textContent = stepsPerCmd;
  cells[7].textContent = cmdsPerContext;
  cells[8].textContent = books;
  cells[9].innerHTML = project.in_cache ? 
    '<span style="color: #4CAF50;">●</span>' : 
    '<span style="color: #999;">○</span>';
    
  // Add animation class
  row.classList.add('recently-updated');
    
  // Remove animation class after it completes
  setTimeout(() => {
    row.classList.remove('recently-updated');
  }, 2000);
}

// Phase 1: Load projects list quickly
async function loadProjects(retryCount = 0) {
  try {
    // Add timeout to prevent hanging during cache warming
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
        
    const response = await fetch('/api/projects?include_stats=true', {
      signal: controller.signal
    });
    clearTimeout(timeoutId);
        
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
        
    const data = await response.json();
    allProjects = data.projects || [];
        
    console.log(`[Overview] Loaded ${allProjects.length} projects`);
        
    // Mark projects that already have stats as processed
    allProjects.forEach(project => {
      if (project.stats !== null && project.stats !== undefined) {
        processedProjects.add(project.log_path);
      }
    });
        
    // Update header with project count (keep simple for header)
    const regularProjectsCount = allProjects.filter(p => p.is_rollup !== true).length;
    document.getElementById('project-count').textContent = regularProjectsCount;
        
    // Initialize filtered projects and render table
    filteredProjects = [...allProjects];
    console.log('[Overview] About to render table after loading projects');
    renderProjectsTable();
        
    // Calculate quick stats from cached projects
    calculateQuickStats(allProjects);
        
  } catch (error) {
    console.error('[Overview] Error loading projects:', error);
        
    // Retry up to 3 times with exponential backoff
    if (retryCount < 3) {
      const delay = Math.pow(2, retryCount) * 1000; // 1s, 2s, 4s
      console.log(`[Overview] Retrying in ${delay/1000}s (attempt ${retryCount + 1}/3)...`);
            
      setTimeout(() => {
        loadProjects(retryCount + 1);
      }, delay);
    } else {
      showError('Failed to load projects');
    }
  }
}

// Calculate quick stats from cached projects
function calculateQuickStats(projects) {
  // Find earliest date (approximation from file times)
  let earliestDate = null;
  let projectsWithStats = 0;
    
  projects.forEach(project => {
    // Track earliest date from file metadata
    if (!earliestDate || project.first_seen < earliestDate) {
      earliestDate = project.first_seen;
    }
        
    // Count projects with stats
    if (project.stats) {
      projectsWithStats++;
    }
  });
    
  // Update header with first use date
  if (earliestDate) {
    const date = new Date(earliestDate * 1000);
    const dateStr = date.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric', 
      year: 'numeric' 
    });
    document.getElementById('first-use-header').textContent = dateStr;
  }
    
  // Note if not all projects have stats
  if (projectsWithStats < projects.length) {
    console.log(`[Overview] Only ${projectsWithStats}/${projects.length} projects have cached stats`);
  }
}

// Filter projects based on search
function filterProjects() {
  const searchTerm = document.getElementById('projects-search').value.toLowerCase();
    
  if (!searchTerm) {
    filteredProjects = [...allProjects];
  } else {
    filteredProjects = allProjects.filter(project => 
      project.display_name.toLowerCase().includes(searchTerm)
    );
  }
    
  // Reset to page 1 when filtering
  currentProjectsPage = 1;
  renderProjectsTable();
}

// Sort projects table
function sortProjectsTable(column) {
  // Toggle sort direction if clicking same column
  if (sortColumn === column) {
    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
  } else {
    sortColumn = column;
    sortDirection = column === 'last_modified' ? 'desc' : 'asc';
  }
    
  // Update sort indicators
  document.querySelectorAll('#projects-table th').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
  });
  const currentTh = document.querySelector(`#projects-table th[onclick*="'${column}'"]`);
  if (currentTh) {
    currentTh.classList.add(sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
  }
    
  renderProjectsTable();
}

// Change projects page
function changeProjectsPage(delta) {
  const totalPages = Math.ceil(filteredProjects.length / projectsPerPage);
  const newPage = currentProjectsPage + delta;
    
  if (newPage >= 1 && newPage <= totalPages) {
    currentProjectsPage = newPage;
    renderProjectsTable();
  }
}

// Go to specific projects page
function goToProjectsPage() {
  const input = document.getElementById('projects-page-input');
  const page = parseInt(input.value);
  const totalPages = Math.ceil(filteredProjects.length / projectsPerPage);
    
  if (page >= 1 && page <= totalPages) {
    currentProjectsPage = page;
    renderProjectsTable();
  } else {
    input.value = currentProjectsPage;
  }
}

// Update projects per page
function updateProjectsPerPage() {
  projectsPerPage = parseInt(document.getElementById('projects-per-page').value);
  currentProjectsPage = 1;
  renderProjectsTable();
}

// Get sort value for a project
function getProjectSortValue(project, column) {
  const stats = project.stats || {};
    
  switch(column) {
  case 'display_name':
    return project.display_name.toLowerCase();
  case 'last_modified':
    return project.last_modified;
  case 'duration':
    if (!stats.first_message_date || !stats.last_message_date) {return -1;}
    return new Date(stats.last_message_date) - new Date(stats.first_message_date);
  case 'cost':
    return stats.total_cost || 0;
  case 'commands':
    return stats.total_commands || 0;
  case 'tokens_per_cmd':
    return stats.avg_tokens_per_command || 0;
  case 'steps_per_cmd':
    return stats.avg_steps_per_command || 0;
  case 'cmds_per_context':
    if (!stats.compact_summary_count || stats.compact_summary_count === 0) return 999999; // Sort to end if no context summaries
    return stats.total_commands / stats.compact_summary_count;
  case 'books':
    if (!stats.total_commands || !stats.avg_tokens_per_command) return 0;
    return (stats.total_commands * stats.avg_tokens_per_command * 3 / 4) / 60000;
  default:
    return 0;
  }
}

// Render projects table with pagination and sorting
function renderProjectsTable() {
  const tbody = document.getElementById('projects-tbody');
  console.log(`[Overview] renderProjectsTable called with ${allProjects.length} projects, ${filteredProjects.length} filtered`);
    
  if (!tbody) {
    console.error('[Overview] projects-tbody element not found!');
    return;
  }
    
  if (allProjects.length === 0) {
    tbody.innerHTML = `
            <tr>
                <td colspan="10">
                    <div class="empty-state">
                        <h2>No Projects Found</h2>
                        <p>Start using Claude Code to see your analytics here.</p>
                    </div>
                </td>
            </tr>
        `;
    document.getElementById('projects-pagination').style.display = 'none';
    return;
  }
    
  // Sort projects
  const sortedProjects = [...filteredProjects].sort((a, b) => {
    const aVal = getProjectSortValue(a, sortColumn);
    const bVal = getProjectSortValue(b, sortColumn);
        
    if (aVal < bVal) {return sortDirection === 'asc' ? -1 : 1;}
    if (aVal > bVal) {return sortDirection === 'asc' ? 1 : -1;}
    return 0;
  });
    
  // Calculate pagination
  const totalPages = Math.ceil(filteredProjects.length / projectsPerPage);
  const startIndex = (currentProjectsPage - 1) * projectsPerPage;
  const endIndex = Math.min(startIndex + projectsPerPage, filteredProjects.length);
  const pageProjects = sortedProjects.slice(startIndex, endIndex);
    
  // Update project count info
  const countInfo = document.getElementById('project-count-info');
  
  // Count rollups vs regular projects
  const rollups = allProjects.filter(p => p.is_rollup === true);
  const regularProjects = allProjects.filter(p => p.is_rollup !== true);
  const filteredRollups = filteredProjects.filter(p => p.is_rollup === true);
  const filteredRegularProjects = filteredProjects.filter(p => p.is_rollup !== true);
  
  console.log('[Overview] Rollup count debug:', {
    totalProjects: allProjects.length,
    rollups: rollups.length,
    regularProjects: regularProjects.length,
    rollupsFound: rollups.map(r => r.display_name)
  });
  
  if (filteredProjects.length !== allProjects.length) {
    // When filtering is active
    let countText = `Showing ${filteredRegularProjects.length} of ${regularProjects.length} projects`;
    if (rollups.length > 0) {
      countText += `, <a href="#" onclick="showCreateRollupModal(); return false;" class="rollup-count-link">${filteredRollups.length} of ${rollups.length} rollups</a>`;
    } else {
      countText += `, <a href="#" onclick="showCreateRollupModal(); return false;" class="rollup-count-link">0 rollups</a>`;
    }
    countInfo.innerHTML = countText;
  } else {
    // When showing all
    let countText = `${regularProjects.length} projects`;
    if (rollups.length > 0) {
      countText += `, <a href="#" onclick="showCreateRollupModal(); return false;" class="rollup-count-link">${rollups.length} rollups</a>`;
    } else {
      countText += `, <a href="#" onclick="showCreateRollupModal(); return false;" class="rollup-count-link">0 rollups</a>`;
    }
    countInfo.innerHTML = countText;
  }
    
  // Render rows
  tbody.innerHTML = pageProjects.map(project => {
    const lastModified = new Date(project.last_modified * 1000);
    const stats = project.stats || {};
    const hasStats = project.stats !== null;
        
    // Calculate duration if dates are available
    let duration = '-';
    if (hasStats && stats.first_message_date && stats.last_message_date) {
      const firstDate = new Date(stats.first_message_date);
      const lastDate = new Date(stats.last_message_date);
      const diffMs = lastDate - firstDate;
      // Add 1 to make it inclusive (e.g., Jun 26 to Jun 28 = 3 days)
      const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24)) + 1;
            
      if (diffDays === 1) {
        duration = '1 day';
      } else if (diffDays < 30) {
        duration = `${diffDays} days`;
      } else if (diffDays < 365) {
        const months = Math.floor(diffDays / 30);
        duration = months === 1 ? '1 month' : `${months} months`;
      } else {
        const years = Math.floor(diffDays / 365);
        duration = years === 1 ? '1 year' : `${years} years`;
      }
    }
        
    // Format cost
    const cost = hasStats && stats.total_cost ? 
      `$${stats.total_cost.toFixed(2)}` : '-';
    
    // Calculate tokens per command
    let tokensPerCmd = '-';
    if (hasStats && stats.avg_tokens_per_command !== undefined) {
      tokensPerCmd = stats.avg_tokens_per_command.toFixed(1);
    }
    
    // Calculate steps per command
    let stepsPerCmd = '-';
    if (hasStats && stats.avg_steps_per_command !== undefined) {
      stepsPerCmd = stats.avg_steps_per_command.toFixed(1);
    }
    
    // Calculate commands per context
    let cmdsPerContext = '-';
    if (hasStats && stats.compact_summary_count > 0 && stats.total_commands > 0) {
      const contextRatio = Math.floor(stats.total_commands / stats.compact_summary_count);
      cmdsPerContext = contextRatio.toString();
    }
    
    // Calculate books
    let books = '-';
    if (hasStats && stats.total_commands && stats.avg_tokens_per_command) {
      const totalWords = stats.total_commands * stats.avg_tokens_per_command * 3 / 4;
      const bookCount = totalWords / 60000;
      books = bookCount >= 1 ? bookCount.toFixed(1) : bookCount.toFixed(2);
    }
        
    // Check if this row was recently updated
    const wasUpdated = projectUpdateMap.has(project.log_path || project.dir_name);
    const isRollup = project.is_rollup === true;
    let rowClass = wasUpdated ? 'recently-updated' : '';
    if (isRollup) rowClass += ' rollup-row';
    
    // For rollups, use a different data attribute
    const dataAttr = isRollup ? `data-rollup-name="${project.rollup_name}"` : `data-project-path="${project.log_path}"`;
        
    return `
            <tr onclick="navigateToProject('${project.url_slug}')" style="cursor: pointer;" class="${rowClass}" ${dataAttr}>
                <td class="project-name" title="${escapeHtml(project.display_name)}">
                    ${isRollup ? '<span class="rollup-icon">📁</span> ' : ''}${escapeHtml(project.display_name)}
                    ${isRollup ? ` <span class="rollup-badge">(${project.child_count} projects)</span>` : ''}
                </td>
                <td>${formatDate(lastModified.toISOString())}</td>
                <td>${duration}</td>
                <td>${cost}</td>
                <td>${hasStats ? formatNumber(stats.total_commands || 0) : '-'}</td>
                <td>${tokensPerCmd}</td>
                <td>${stepsPerCmd}</td>
                <td>${cmdsPerContext}</td>
                <td>${books}</td>
                <td>
                    ${project.in_cache ? 
    '<span style="color: #4CAF50;">●</span>' : 
    '<span style="color: #999;">○</span>'}
                </td>
            </tr>
        `;
  }).join('');
    
  // Update pagination controls
  if (totalPages > 1) {
    document.getElementById('projects-pagination').style.display = 'flex';
    document.getElementById('projects-page-input').value = currentProjectsPage;
    document.getElementById('projects-total-pages').textContent = totalPages;
    document.getElementById('projects-prev-btn').disabled = currentProjectsPage === 1;
    document.getElementById('projects-next-btn').disabled = currentProjectsPage === totalPages;
  } else {
    document.getElementById('projects-pagination').style.display = 'none';
  }
}

// Navigate to project dashboard
function navigateToProject(urlSlug) {
  if (urlSlug.startsWith('rollup:')) {
    // For rollups, use the rollup endpoint
    const rollupName = urlSlug.substring(7); // Remove 'rollup:' prefix
    window.location.href = `/rollup/${rollupName}`;
  } else {
    window.location.href = `/project/${urlSlug}`;
  }
}

// Rollup management functions
function showCreateRollupModal() {
  document.getElementById('create-rollup-modal').style.display = 'block';
  // Focus the name input
  document.getElementById('rollup-name').focus();
}

function hideCreateRollupModal() {
  document.getElementById('create-rollup-modal').style.display = 'none';
  // Clear form
  document.getElementById('create-rollup-form').reset();
}

async function createRollup() {
  const name = document.getElementById('rollup-name').value.trim();
  const path = document.getElementById('rollup-path').value.trim();
  
  if (!name || !path) {
    alert('Please fill in both rollup name and directory path.');
    return;
  }
  
  try {
    const response = await fetch('/api/rollups', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ name, path })
    });
    
    const result = await response.json();
    
    if (response.ok) {
      console.log('[Overview] Rollup created successfully:', result);
      hideCreateRollupModal();
      // Reload projects to show the new rollup
      await loadProjects();
    } else {
      alert(`Failed to create rollup: ${result.detail || result.error || 'Unknown error'}`);
    }
  } catch (error) {
    console.error('[Overview] Error creating rollup:', error);
    alert('Failed to create rollup. Please try again.');
  }
}

// Close modal when clicking outside of it
window.onclick = function(event) {
  const modal = document.getElementById('create-rollup-modal');
  if (event.target === modal) {
    hideCreateRollupModal();
  }
}

// Phase 2: Load and aggregate global statistics
async function loadGlobalStats() {
  try {
    // Load global statistics from API
    const response = await fetch('/api/global-stats');
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
        
    const globalStats = await response.json();
    console.log('[Overview] Loaded global stats:', globalStats);
        
    // Store configuration
    if (globalStats.config) {
      window.maxDateRangeDays = globalStats.config.max_date_range_days || 30;
    }
        
    // Store full global stats for date range updates
    fullGlobalStats = globalStats;
        
    // Update UI with real data
    updateGlobalStatsUI(globalStats);
        
    // Initialize date range pickers if not already done
    if (!overviewTokenDatePicker && !overviewCostDatePicker) {
      initializeDatePickers(globalStats);
    }
        
    // Create a simple hash of the data to detect changes
    const currentDataHash = JSON.stringify({
      tokenData: globalStats.daily_token_usage.map(d => ({ 
        date: d.date, 
        input: d.input, 
        output: d.output 
      })),
      costData: globalStats.daily_costs.map(d => ({ 
        date: d.date, 
        cost: d.cost,
        input_cost: d.input_cost || 0,
        output_cost: d.output_cost || 0,
        cache_cost: d.cache_cost || 0
      }))
    });
        
    // Only render charts if data has changed
    if (currentDataHash !== lastChartDataHash) {
      console.log('[Overview] Chart data changed, rendering charts');
      // Update the full stats reference
      fullGlobalStats = globalStats;
      // Re-render charts with current date range
      if (overviewTokenDatePicker && overviewCostDatePicker) {
        const tokenRange = overviewTokenDatePicker.getDateRange();
        const costRange = overviewCostDatePicker.getDateRange();
        renderTokenUsageChartWithDateRange(tokenRange.startDate, tokenRange.endDate);
        renderCostTrendChartWithDateRange(costRange.startDate, costRange.endDate);
      } else {
        renderTokenUsageChartWithDateRange();
        renderCostTrendChartWithDateRange();
      }
      lastChartDataHash = currentDataHash;
    } else {
      console.log('[Overview] Chart data unchanged, skipping render');
    }
        
  } catch (error) {
    console.error('[Overview] Error loading global stats:', error);
    // Continue showing data from projects we already loaded
  }
}

// Start periodic chart updates
function startChartUpdates() {
  // Only start chart updates if background updates are actually needed
  const projectsWithoutStats = allProjects.filter(p => p.stats === null).length;
    
  if (projectsWithoutStats === 0) {
    console.log('[Overview] All projects already have stats, no chart updates needed');
    return;
  }
    
  // Update charts every 2 seconds if there are projects being processed
  chartUpdateInterval = setInterval(async () => {
    // Check if background updates are still running
    if (!updateCheckInterval) {
      console.log('[Overview] Background updates stopped, stopping chart updates');
      clearInterval(chartUpdateInterval);
      chartUpdateInterval = null;
      return;
    }
        
    // Only update if we have projects that might have new stats
    const currentProjectsWithoutStats = allProjects.filter(p => p.stats === null).length;
    const projectsBeingProcessed = allProjects.length - processedProjects.size;
        
    if (currentProjectsWithoutStats > 0 || projectsBeingProcessed > 0) {
      console.log('[Overview] Updating charts with latest data');
      await loadGlobalStats();
    } else {
      console.log('[Overview] All projects processed, stopping chart updates');
      clearInterval(chartUpdateInterval);
      chartUpdateInterval = null;
    }
  }, 2000); // Update every 2 seconds (same as project updates)
}

// Format cost for display
function formatCost(cost) {
  if (cost < 0.01) {
    return `$${cost.toFixed(4)}`;
  } else if (cost < 1) {
    return `$${cost.toFixed(3)}`;
  } else {
    return `$${cost.toFixed(2)}`;
  }
}

// Check if user has been using Claude Code for more than 30 days
function checkIfMoreThan30DaysUsage(stats) {
  if (!stats.first_use_date) {return false;}
    
  const firstUseDate = new Date(stats.first_use_date);
  const today = new Date();
  const daysDiff = (today - firstUseDate) / (1000 * 60 * 60 * 24);
    
  return daysDiff > 30;
}

// Update UI with global statistics
function updateGlobalStatsUI(stats) {
  // Update first use date with better accuracy
  if (stats.first_use_date) {
    const firstUse = new Date(stats.first_use_date);
    const dateStr = firstUse.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric', 
      year: 'numeric' 
    });
        
    // Update header with first use date
    document.getElementById('first-use-header').textContent = dateStr;
  }
    
  // Check if user has been using for more than 30 days
  const hasMoreThan30Days = checkIfMoreThan30DaysUsage(stats);
    
  // Update cost summary
  if (stats.total_cost !== undefined) {
    const allTimeCost = stats.total_cost || 0;
    document.getElementById('total-cost-all-time').textContent = 
            `All-time: ${formatCost(allTimeCost)}`;
  }
    
  // Calculate 30-day total from daily costs - only show if user has >30 days of usage
  const cost30Days = document.getElementById('total-cost-30-days');
  if (cost30Days) {
    if (hasMoreThan30Days && stats.daily_costs) {
      const thirtyDayTotal = stats.daily_costs.reduce((sum, day) => sum + (day.cost || 0), 0);
      cost30Days.textContent = `30-day: ${formatCost(thirtyDayTotal)}`;
      cost30Days.style.display = 'inline';
      // Show separator if 30-day is visible
      const separator = cost30Days.previousElementSibling;
      if (separator && separator.classList.contains('separator')) {
        separator.style.display = 'inline';
      }
    } else {
      cost30Days.style.display = 'none';
      // Hide separator if 30-day is hidden
      const separator = cost30Days.previousElementSibling;
      if (separator && separator.classList.contains('separator')) {
        separator.style.display = 'none';
      }
    }
  }
    
  // Update token summary
  // All-time tokens
  const allTimeTokens = document.getElementById('token-all-time');
  if (allTimeTokens) {
    const inputTokens = stats.total_input_tokens || 0;
    const outputTokens = stats.total_output_tokens || 0;
        
    allTimeTokens.innerHTML = `All-time: <span class="token-input">${formatNumber(inputTokens)}</span> input • ` +
            `<span class="token-output">${formatNumber(outputTokens)}</span> output`;
  }
    
  // 30-day tokens - only show if user has >30 days of usage
  const tokens30Days = document.getElementById('token-30-days');
  if (tokens30Days) {
    if (hasMoreThan30Days && stats.daily_token_usage) {
      const thirtyDayTokens = stats.daily_token_usage.reduce((acc, day) => {
        acc.input += day.input || 0;
        acc.output += day.output || 0;
        return acc;
      }, { input: 0, output: 0 });
            
      tokens30Days.innerHTML = `30-day: <span class="token-input">${formatNumber(thirtyDayTokens.input)}</span> input • ` +
                `<span class="token-output">${formatNumber(thirtyDayTokens.output)}</span> output`;
      tokens30Days.style.display = 'block';
    } else {
      tokens30Days.style.display = 'none';
    }
  }
}

// Initialize date range pickers for charts
function initializeDatePickers(stats) {
  // Find date range from daily data
  const dates = stats.daily_token_usage.map(d => d.date).sort();
  if (dates.length === 0) {return;}
    
  // Get min date from the earliest data
  const minDate = dates.length > 0 ? new Date(dates[0]) : null;
    
  // Initialize token chart date picker
  overviewTokenDatePicker = new DateRangePicker({
    containerId: 'overview-token-date-picker',
    minDate: minDate,  // Limit to earliest data date
    maxDate: new Date(),  // Limit to today
    defaultDays: 30,
    maxDays: window.maxDateRangeDays || 30,
    onRangeChange: (range) => {
      renderTokenUsageChartWithDateRange(range.startDate, range.endDate);
    }
  });
    
  // Initialize cost chart date picker
  overviewCostDatePicker = new DateRangePicker({
    containerId: 'overview-cost-date-picker',
    minDate: minDate,  // Limit to earliest data date
    maxDate: new Date(),  // Limit to today
    defaultDays: 30,
    maxDays: window.maxDateRangeDays || 30,
    onRangeChange: (range) => {
      renderCostTrendChartWithDateRange(range.startDate, range.endDate);
    }
  });
}

// Render token usage chart with date range filtering
function renderTokenUsageChartWithDateRange(startDate, endDate) {
  if (!fullGlobalStats) {return;}
    
  let filteredData;
  if (startDate && endDate) {
    // Filter data based on date range using string comparison
    filteredData = fullGlobalStats.daily_token_usage.filter(day => {
      return day.date >= startDate && day.date <= endDate;
    });
  } else {
    // Default to last 30 days
    filteredData = fullGlobalStats.daily_token_usage.slice(-30);
  }
    
  renderTokenUsageChart(filteredData);
}

// Render cost trend chart with date range filtering
function renderCostTrendChartWithDateRange(startDate, endDate) {
  if (!fullGlobalStats) {return;}
    
  let filteredData;
  if (startDate && endDate) {
    // Filter data based on date range using string comparison
    filteredData = fullGlobalStats.daily_costs.filter(day => {
      return day.date >= startDate && day.date <= endDate;
    });
  } else {
    // Default to last 30 days
    filteredData = fullGlobalStats.daily_costs.slice(-30);
  }
    
  renderCostTrendChart(filteredData);
}

// Render token usage chart
function renderTokenUsageChart(data) {
  const ctx = document.getElementById('token-usage-chart').getContext('2d');
    
  if (tokenUsageChart) {
    tokenUsageChart.destroy();
  }
    
  // Parse dates directly to avoid timezone issues
  const labels = data.map(d => {
    const [year, month, day] = d.date.split('-');
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[parseInt(month) - 1];
    return `${monthName} ${parseInt(day)}`;
  });
    
  tokenUsageChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {
          label: 'Input Tokens',
          data: data.map(d => d.input || 0),
          backgroundColor: 'rgba(102, 126, 234, 0.8)',
          borderColor: 'rgba(102, 126, 234, 1)',
          borderWidth: 1
        },
        {
          label: 'Output Tokens',
          data: data.map(d => d.output || 0),
          backgroundColor: 'rgba(118, 75, 162, 0.8)',
          borderColor: 'rgba(118, 75, 162, 1)',
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false
      },
      scales: {
        x: {
          stacked: true,
          grid: { display: false }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              return formatNumber(value);
            }
          }
        }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: function(context) {
              return context.dataset.label + ': ' + formatNumber(context.parsed.y);
            }
          }
        }
      }
    }
  });
}

// Render cost trend chart
function renderCostTrendChart(data) {
  const ctx = document.getElementById('cost-trend-chart').getContext('2d');
    
  if (costTrendChart) {
    costTrendChart.destroy();
  }
    
  // Extract data for each cost component
  // Parse dates directly to avoid timezone issues
  const labels = data.map(d => {
    const [year, month, day] = d.date.split('-');
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[parseInt(month) - 1];
    return `${monthName} ${parseInt(day)}`;
  });
  const inputCosts = data.map(d => d.input_cost || 0);
  const outputCosts = data.map(d => d.output_cost || 0);
  const cacheCosts = data.map(d => d.cache_cost || 0);
    
  costTrendChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {
          label: 'Input Tokens',
          data: inputCosts,
          backgroundColor: '#667eea',
          stack: 'cost'
        },
        {
          label: 'Output Tokens',
          data: outputCosts,
          backgroundColor: '#764ba2',
          stack: 'cost'
        },
        {
          label: 'Cache Operations',
          data: cacheCosts,
          backgroundColor: '#48bb78',
          stack: 'cost'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false
      },
      scales: {
        x: {
          stacked: true,
          grid: { display: false }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              if (value < 0.01) {
                return '$' + value.toFixed(4);
              } else if (value < 1) {
                return '$' + value.toFixed(3);
              } else {
                return '$' + value.toFixed(2);
              }
            }
          }
        }
      },
      plugins: {
        tooltip: {
          callbacks: {
            footer: function(tooltipItems) {
              let sum = 0;
              tooltipItems.forEach(function(tooltipItem) {
                sum += tooltipItem.parsed.y;
              });
              return 'Total: $' + sum.toFixed(4);
            }
          }
        }
      }
    }
  });
}

// Show error message
function showError(message) {
  const tbody = document.getElementById('projects-tbody');
  tbody.innerHTML = `
        <tr>
            <td colspan="9">
                <div class="empty-state">
                    <h2>Error</h2>
                    <p>${escapeHtml(message)}</p>
                </div>
            </td>
        </tr>
    `;
}

// Refresh data function - only processes changed files
async function refreshData() {
  const button = document.getElementById('refresh-button');
  const originalText = button.innerHTML;
    
  // Start timing
  const refreshStart = performance.now();
  console.log('[Overview] 🔄 Starting data refresh...');
    
  try {
    button.innerHTML = '⏳ Refreshing...';
    button.disabled = true;
        
    // Call the refresh endpoint
    const timezoneOffset = new Date().getTimezoneOffset();
    const response = await fetch('/api/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timezone_offset: -timezoneOffset })
    });
        
    const refreshTime = performance.now() - refreshStart;
        
    if (!response.ok) {
      throw new Error('Refresh failed');
    }
        
    const result = await response.json();
    console.log(`[Overview] ✓ Data refreshed in ${refreshTime.toFixed(2)}ms`);
        
    if (result.files_changed) {
      console.log('[Overview]   - Files changed: Yes');
      console.log(`[Overview]   - Projects refreshed: ${result.projects_refreshed || 'N/A'} of ${result.total_projects || 'N/A'}`);
      button.innerHTML = '✅ Updated!';
            
      // Reload the page to show new data
      setTimeout(() => {
        window.location.reload();
      }, 500);
    } else {
      console.log('[Overview]   - Files changed: No (using cached data)');
      button.innerHTML = '✅ No changes!';
            
      // Reset button after showing no changes
      setTimeout(() => {
        button.innerHTML = originalText;
        button.disabled = false;
      }, 2000);
    }
        
  } catch (error) {
    const refreshTime = performance.now() - refreshStart;
    console.error(`[Overview] ✗ Refresh error after ${refreshTime.toFixed(2)}ms:`, error);
        
    button.innerHTML = '❌ Error!';
        
    // Reset button after error
    setTimeout(() => {
      button.innerHTML = originalText;
      button.disabled = false;
    }, 2000);
  }
}

