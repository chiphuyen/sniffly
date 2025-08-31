// Chart functions for Claude Analytics Dashboard
// This module contains all chart initialization and configuration
//
// IMPORTANT: Chart Time Range Behavior
// =====================================
// Charts in the dashboard display data with different time ranges:
//
// 1. LAST 30 DAYS ONLY (will show empty/zeros for inactive projects):
//    - Token Usage Over Time (tokens-chart) - shows daily token usage for last 30 days
//    - Daily Cost Breakdown (daily-cost-chart) - shows daily costs for last 30 days
//
// 2. ALL-TIME DATA (will show historical data even for old projects):
//    - Tool Usage (tools-chart) - shows total usage counts for all tools
//    - Token Usage by Hour (hourly-tokens-chart) - shows hourly patterns across all time
//    - User Interactions (interruption-rate-chart) - shows command complexity distribution
//    - Model Usage (model-usage-chart) - shows token usage by model across all time
//    - Error Distribution (error-distribution-chart) - shows error categories across all time
//
// 3. DYNAMIC TIME RANGE (shows data from first to last activity):
//    - Command Complexity Over Time (command-complexity-chart) - adapts to project duration
//    - Tool Usage Trends (tool-trends-chart) - adapts to project duration
//
// For projects inactive for >30 days, the "Last 30 Days" charts will appear empty
// while the "All-Time" and "Dynamic" charts will still show historical data.

// Chart instances storage
let chartInstances = {
  tokens: null,
  tools: null,
  hourlyTokens: null,
  userInteractions: null,
  modelUsage: null,
  errorDistribution: null,
  commandComplexity: null,
  commandLength: null,
  toolTrends: null,
  dailyCost: null,
  dailyInterruption: null
};

// Store full statistics for date range updates
let fullStatistics = null;

// Date range pickers
let tokenDatePicker = null;
let costDatePicker = null;

// Main chart initialization function
async function initializeCharts(statistics) {
  // Store full statistics for date range updates
  fullStatistics = statistics;
  
  // Load dynamic pricing before initializing cost charts
  if (window.PricingUtils && !window.PricingUtils.PRICING_LOAD_ATTEMPTED) {
    await window.PricingUtils.loadDynamicPricing();
  }
  
  // Initialize date range pickers for 30-day charts
  initializeDatePickers(statistics);
    
  // (Messages over time chart removed)
    
  // Token usage over time - LAST 30 DAYS ONLY
  // Note: This chart will show empty/zero values for projects inactive >30 days
  renderTokenChart(); // Use the new render function
    
  // Tool usage - ALL-TIME DATA
  // Note: Shows cumulative tool usage across entire project history
  const toolsData = Object.entries(statistics.tools.usage_counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10);
    
  chartInstances.tools = new Chart(document.getElementById('tools-chart'), {
    type: 'bar',
    data: {
      labels: toolsData.map(([tool]) => tool),
      datasets: [{
        label: 'Usage Count',
        data: toolsData.map(([, count]) => count),
        backgroundColor: '#667eea'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: {
        legend: { display: false }
      },
      scales: {
        x: {
          beginAtZero: true
        }
      }
    }
  });
    
  // (Activity by hour chart removed)
    
  // Token usage by hour - ALL-TIME DATA
  // Note: Shows aggregated hourly patterns across entire project history
  const hourlyPattern = statistics.hourly_pattern || {};
  const hourlyTokenData = hourlyPattern.tokens || {};
  
  // Extract hourly token arrays
  const inputTokensByHour = [];
  const outputTokensByHour = [];
  
  for (let hour = 0; hour < 24; hour++) {
    const hourData = hourlyTokenData[hour] || {};
    inputTokensByHour.push(hourData.input || 0);
    outputTokensByHour.push(hourData.output || 0);
  }
    
  chartInstances.hourlyTokens = new Chart(document.getElementById('hourly-tokens-chart'), {
    type: 'bar',
    data: {
      labels: Array.from({length: 24}, (_, i) => `${i}:00`),
      datasets: [
        {
          label: 'Input Tokens',
          data: inputTokensByHour,
          backgroundColor: '#667eea'
        },
        {
          label: 'Output Tokens',
          data: outputTokensByHour,
          backgroundColor: '#764ba2'
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { stacked: true },
        y: { 
          stacked: true,
          beginAtZero: true
        }
      },
      plugins: {
        legend: {
          position: 'bottom'
        }
      }
    }
  });
    
  // User interactions chart (if data available)
  if (statistics.user_interactions && statistics.user_interactions.tool_count_distribution) {
    const distributionData = Object.entries(statistics.user_interactions.tool_count_distribution)
      .sort((a, b) => parseInt(a[0]) - parseInt(b[0]));
        
    // Get commands with 0 tools
    const zeroToolCommands = distributionData.find(([count]) => count === '0')?.[1] || 0;
        
    // Filter out zero tools and create data for tools >= 1
    const filteredData = distributionData.filter(([count]) => parseInt(count) > 0);
        
    // Find the max number of tools to set x-axis range
    const maxTools = filteredData.length > 0 
      ? Math.max(...filteredData.map(([count]) => parseInt(count)))
      : 0;
        
    // Skip chart if no data
    // if (maxTools === 0 || filteredData.length === 0) {
    //   const chartElement = document.getElementById('user-interactions-chart');
    //   if (chartElement && chartElement.parentElement) {
    //     chartElement.parentElement.style.display = 'none';
    //   }
    //   return;
    // }
        
    // Create labels for x-axis (1 to max tools)
    const labels = Array.from({length: maxTools}, (_, i) => (i + 1).toString());
        
    // Calculate total non-interruption commands
    const totalCommands = Object.values(statistics.user_interactions.tool_count_distribution)
      .reduce((sum, count) => sum + count, 0);
        
    // Create data array as percentages
    const data = new Array(maxTools).fill(0);
    filteredData.forEach(([toolCount, commandCount]) => {
      const index = parseInt(toolCount) - 1; // Adjust index since we start from 1
      if (index >= 0) {
        data[index] = (commandCount / totalCommands * 100);
      }
    });
        
    // Update the chart title to show total N
    const chartContainer = document.getElementById('user-interactions-chart').parentElement;
    const chartTitle = chartContainer.querySelector('h2');
    chartTitle.innerHTML = `User Command Analysis <span style="font-size: 0.9rem; color: #666; font-weight: normal;">(N=${totalCommands})</span>`;
        
    // Add info text about zero tools
    const infoText = document.createElement('div');
    infoText.style.textAlign = 'center';
    infoText.style.marginTop = '-15px';
    infoText.style.marginBottom = '10px';
    infoText.style.fontSize = '0.9rem';
    infoText.style.color = '#666';
    const zeroToolPercentage = ((zeroToolCommands / totalCommands) * 100).toFixed(1);
    infoText.textContent = `${zeroToolPercentage}% of commands used 0 tools`;
    chartContainer.insertBefore(infoText, chartContainer.querySelector('canvas'));
        
    chartInstances.userInteractions = new Chart(document.getElementById('user-interactions-chart'), {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: 'Commands',
          data: data,
          backgroundColor: '#667eea',
          borderColor: '#5a67d8',
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            title: {
              display: true,
              text: 'Number of Tools Used'
            },
            ticks: {
              stepSize: 1
            }
          },
          y: {
            title: {
              display: true,
              text: 'Percentage of Commands'
            },
            beginAtZero: true,
            ticks: {
              callback: function(value) {
                return value.toFixed(1) + '%';
              }
            }
          }
        },
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            callbacks: {
              title: function(context) {
                const tools = context[0].label;
                return tools === '1' ? '1 tool' : `${tools} tools`;
              },
              label: function(context) {
                const percentage = context.parsed.y;
                const toolCount = parseInt(context.label);
                const commandCount = filteredData.find(([count]) => parseInt(count) === toolCount)?.[1] || 0;
                return [
                  `${percentage.toFixed(1)}% of commands`,
                  `(${commandCount} commands)`
                ];
              }
            }
          }
        }
      }
    });
  }
    
    
  // Model usage pie chart
  if (statistics.user_interactions && statistics.user_interactions.model_distribution) {
    const modelData = Object.entries(statistics.user_interactions.model_distribution)
      .filter(([model, count]) => model && model !== 'N/A' && count > 0)
      .sort((a, b) => b[1] - a[1]);
        
    if (modelData.length > 0) {
      // Check if <synthetic> tag appears in model names
      const hasSyntheticTag = modelData.some(([model]) => model.includes('<synthetic>'));
            
      const colors = [
        '#667eea', '#48bb78', '#ed8936', '#e53e3e', '#38b2ac', 
        '#d69e2e', '#805ad5', '#3182ce', '#dd6b20', '#319795'
      ];
            
      chartInstances.modelUsage = new Chart(document.getElementById('model-usage-chart'), {
        type: 'pie',
        data: {
          labels: modelData.map(([model]) => model),
          datasets: [{
            data: modelData.map(([_, count]) => count),
            backgroundColor: colors.slice(0, modelData.length),
            borderWidth: 2,
            borderColor: '#fff'
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              position: 'right',
              labels: {
                padding: 15,
                font: {
                  size: 12
                }
              }
            },
            tooltip: {
              callbacks: {
                afterLabel: function(context) {
                  const total = context.dataset.data.reduce((a, b) => a + b, 0);
                  const percentage = ((context.parsed / total) * 100).toFixed(1);
                  return `${percentage}% of commands`;
                }
              }
            }
          }
        }
      });
            
      // Show tooltip icon if synthetic tag is present
      if (hasSyntheticTag) {
        const tooltipIcon = document.getElementById('synthetic-tooltip-icon');
        if (tooltipIcon) {
          tooltipIcon.style.display = 'inline';
        }
      }
    } else {
      // Hide the chart container if no data
      document.getElementById('model-usage-chart').parentElement.style.display = 'none';
    }
  } else {
    // Hide the chart container if no data
    document.getElementById('model-usage-chart').parentElement.style.display = 'none';
  }
    
  // Error Distribution Chart
  if (statistics.errors && statistics.errors.by_category) {
    const errorData = Object.entries(statistics.errors.by_category)
      .filter(([category, count]) => category !== 'User Interruption' && count > 0)
      .sort((a, b) => b[1] - a[1]);
        
    if (errorData.length > 0) {
      const colors = [
        '#e53e3e', '#ed8936', '#d69e2e', '#48bb78', '#38b2ac',
        '#3182ce', '#667eea', '#805ad5', '#d53f8c', '#718096'
      ];
            
      // Ensure we have enough colors
      const backgroundColors = [];
      for (let i = 0; i < errorData.length; i++) {
        backgroundColors.push(colors[i % colors.length]);
      }
            
      chartInstances.errorDistribution = new Chart(document.getElementById('error-distribution-chart'), {
        type: 'doughnut',
        data: {
          labels: errorData.map(([category]) => category),
          datasets: [{
            data: errorData.map(([_, count]) => count),
            backgroundColor: backgroundColors,
            borderWidth: 2,
            borderColor: '#fff'
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          layout: {
            padding: {
              left: 10,
              right: 10,
              top: 0,
              bottom: 10
            }
          },
          plugins: {
            legend: {
              position: 'right',
              align: 'center',  // Center vertically instead of top
              labels: {
                padding: 6,  // More compact
                font: {
                  size: 10  // Smaller font
                },
                boxWidth: 10,  // Smaller color box
                usePointStyle: true,  // Use circle instead of rectangle
                generateLabels: function(chart) {
                  const data = chart.data;
                  const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
                                    
                  return data.labels.map((label, i) => {
                    const value = data.datasets[0].data[i];
                    const percentage = ((value / total) * 100).toFixed(1);
                    // Truncate long labels more aggressively
                    const maxLength = 20;
                    const displayLabel = label.length > maxLength ? 
                      label.substring(0, maxLength - 3) + '...' : label;
                    return {
                      text: `${displayLabel} (${percentage}%)`,
                      fillStyle: data.datasets[0].backgroundColor[i],
                      strokeStyle: '#fff',  // Use white border for legend items
                      lineWidth: data.datasets[0].borderWidth,
                      hidden: false,
                      index: i
                    };
                  });
                }
              }
            },
            tooltip: {
              callbacks: {
                label: function(context) {
                  const total = context.dataset.data.reduce((a, b) => a + b, 0);
                  const value = context.parsed;
                  const percentage = ((value / total) * 100).toFixed(1);
                  return `${context.label}: ${value} errors (${percentage}%)`;
                }
              }
            },
            title: {
              display: true,
              text: `Total Errors: ${statistics.errors.total} (${(statistics.errors.rate * 100).toFixed(1)}% of all messages)`,
              font: {
                size: 14
              },
              padding: {
                top: 0,
                bottom: 15  // Reduced gap
              }
            }
          }
        }
      });
    } else {
      // Hide the chart container if no data
      document.getElementById('error-distribution-chart').parentElement.style.display = 'none';
    }
  } else {
    // Hide the chart container if no data
    document.getElementById('error-distribution-chart').parentElement.style.display = 'none';
  }
    
  // Command complexity over time
  createCommandComplexityChart(statistics);
  
  // Command length over time
  createCommandLengthChart(statistics);
    
  // Tool usage trends
  createToolTrendsChart(statistics);
  
  // Daily Cost Breakdown with date picker
  renderCostChart();
  
  // Interruption Rate Chart
  createInterruptionRateChart(statistics);
  
  // Step-Length Metrics Charts
  createStepLengthCharts(statistics);
  
  // Error Rate Chart
  createErrorRateChart(statistics);
}

// Create command complexity over time chart - DYNAMIC TIME RANGE
// Note: This chart adapts to show data from first to last project activity,
// making it suitable for viewing historical data from inactive projects
function createCommandComplexityChart(statistics) {
  // Get non-interruption commands
  const commandDetails = statistics.user_interactions.command_details || [];
  const nonInterruptionCommands = commandDetails.filter(cmd => !cmd.is_interruption);
    
  if (nonInterruptionCommands.length === 0) {
    // Hide chart if no data
    document.getElementById('command-complexity-chart').parentElement.style.display = 'none';
    return;
  }
    
  // Sort commands by timestamp
  const sortedCommands = nonInterruptionCommands.sort((a, b) => 
    new Date(a.timestamp) - new Date(b.timestamp)
  );
  
  // Get time range
  const startTime = new Date(sortedCommands[0].timestamp);
  const endTime = new Date(sortedCommands[sortedCommands.length - 1].timestamp);
  const totalDuration = endTime - startTime;
  const totalDays = totalDuration / (24 * 60 * 60 * 1000);
  
  // Determine interval: 4 hours if < 10 days, otherwise daily
  const useHourlyInterval = totalDays < 10;
  const intervalMs = useHourlyInterval ? 4 * 60 * 60 * 1000 : 24 * 60 * 60 * 1000;
  
  // Create buckets using Map to preserve order
  const buckets = new Map();
  
  // Helper to get bucket key
  const getBucketKey = (date) => {
    if (useHourlyInterval) {
      // Round to nearest 4-hour interval
      const hours = Math.floor(date.getHours() / 4) * 4;
      date.setHours(hours, 0, 0, 0);
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
    } else {
      // Daily bucket
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    }
  };
  
  // Group commands into buckets
  sortedCommands.forEach(cmd => {
    const cmdDate = new Date(cmd.timestamp);
    const bucketKey = getBucketKey(cmdDate);
    
    if (!buckets.has(bucketKey)) {
      buckets.set(bucketKey, {
        commands: [],
        timestamp: bucketKey
      });
    }
    
    buckets.get(bucketKey).commands.push(cmd);
  });
  
  // Sort bucket keys and calculate cumulative averages
  const sortedBucketKeys = Array.from(buckets.keys()).sort();
  
  let totalCommands = 0;
  let totalTools = 0;
  let totalSteps = 0;
  
  const bucketData = sortedBucketKeys.map(bucketKey => {
    const bucket = buckets.get(bucketKey);
    
    // Add commands from this bucket to cumulative totals
    bucket.commands.forEach(cmd => {
      totalCommands++;
      totalTools += cmd.tools_used || 0;
      totalSteps += cmd.assistant_steps || 0;
    });
    
    // Parse bucket timestamp for display
    let displayTime;
    if (useHourlyInterval) {
      const [year, month, day, hour] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day, hour);
    } else {
      const [year, month, day] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day);
    }
    
    return {
      time: displayTime,
      cumulativeAvgTools: totalCommands > 0 ? totalTools / totalCommands : 0,
      cumulativeAvgSteps: totalCommands > 0 ? totalSteps / totalCommands : 0,
      totalCommands: totalCommands,
      bucketCommands: bucket.commands.length
    };
  });
  
  // Limit to last 60 data points
  const limitedBucketData = bucketData.slice(-60);
    
  // Format labels based on interval type
  const formatLabel = (date) => {
    if (useHourlyInterval) {
      // For 4-hour intervals, show date and time
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        hour: 'numeric',
        hour12: true
      });
    } else {
      // For daily intervals, just show date
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric' 
      });
    }
  };
    
  // Convert to arrays for Chart.js (already filtered by activity)
  const labels = limitedBucketData.map(d => formatLabel(d.time));
  const avgTools = limitedBucketData.map(d => d.cumulativeAvgTools);
  const avgSteps = limitedBucketData.map(d => d.cumulativeAvgSteps);
    
  // Create sparse labels to avoid clutter
  const sparseLabels = labels.map((label, index) => {
    if (labels.length <= 15) {
      // Show all labels if we have few data points
      return label;
    } else {
      // For many data points, show every Nth label
      const step = Math.ceil(labels.length / 15);
      if (index % step === 0 || index === labels.length - 1) {
        return label;
      }
      return '';
    }
  });
    
  // Calculate min/max for better scaling
  const allValues = [...avgTools, ...avgSteps];
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const range = maxValue - minValue;
    
  // Calculate smart bounds
  let yMin, yMax;
  if (range < 2) {
    // For very small ranges, add fixed padding
    yMin = Math.max(0, minValue - 1);
    yMax = maxValue + 1;
  } else {
    // For larger ranges, add percentage padding
    const padding = range * 0.2; // 20% padding
    yMin = Math.max(0, minValue - padding);
    yMax = maxValue + padding;
  }
    
  // Create the Command Complexity chart
  chartInstances.commandComplexity = makeDynamicIntervalChart({
    canvasId: 'command-complexity-chart',
    labels:   sparseLabels,
    datasets: [
      { label: 'avg. tools/cmd', data: avgTools,
        ...withColor('blue'),  tension: 0.3, pointRadius: 2, pointHoverRadius: 5, yAxisID: 'y' },
      { label: 'avg. steps/cmd', data: avgSteps,
        ...withColor('amber'), tension: 0.3, pointRadius: 2, pointHoverRadius: 5, yAxisID: 'y' }
    ],
    yScales: {
      y: {
        type: 'linear', position: 'left',
        beginAtZero: false, min: yMin, max: yMax,
        ticks: { precision: 1 },
        title: { display: true, text: 'Average Count' }
      }
    },
    tooltipExtra: {
      afterTitle(ctx) {
        const d = limitedBucketData[ctx[0].dataIndex];
        return [
          `Total commands: ${d.totalCommands}`,
          `Commands in period: ${d.bucketCommands}`
        ];
      },
      label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(2)}`
    },
    optionOverrides: {
      plugins: { legend: { position: 'top' } }
    },
    limitedBucketData: limitedBucketData,
    useHourlyInterval: useHourlyInterval
  });
}

// Create command length chart - DYNAMIC TIME RANGE
function createCommandLengthChart(statistics) {
  // Get command details sorted by timestamp
  const commandDetails = statistics.user_interactions.command_details || [];
  const nonInterruptionCommands = commandDetails.filter(cmd => !cmd.is_interruption);
  
  if (nonInterruptionCommands.length === 0) {
    // Hide chart if no data
    document.getElementById('command-length-chart').parentElement.style.display = 'none';
    return;
  }
  
  // Sort by timestamp
  const sortedCommands = nonInterruptionCommands.sort((a, b) => 
    new Date(a.timestamp) - new Date(b.timestamp)
  );
  
  // Get time range
  const startTime = new Date(sortedCommands[0].timestamp);
  const endTime = new Date(sortedCommands[sortedCommands.length - 1].timestamp);
  const totalDuration = endTime - startTime;
  const totalDays = totalDuration / (24 * 60 * 60 * 1000);
  
  // Determine interval
  const useHourlyInterval = totalDays < 10;
  
  // Create buckets using Map to preserve order
  const buckets = new Map();
  
  // Helper to get bucket key
  const getBucketKey = (date) => {
    if (useHourlyInterval) {
      // Round to nearest 4-hour interval
      const hours = Math.floor(date.getHours() / 4) * 4;
      date.setHours(hours, 0, 0, 0);
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
    } else {
      // Daily bucket
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    }
  };
  
  // Group commands into buckets
  sortedCommands.forEach(cmd => {
    const cmdDate = new Date(cmd.timestamp);
    const bucketKey = getBucketKey(cmdDate);
    
    if (!buckets.has(bucketKey)) {
      buckets.set(bucketKey, {
        commands: [],
        timestamp: bucketKey
      });
    }
    
    buckets.get(bucketKey).commands.push(cmd);
  });
  
  // Sort bucket keys and calculate averages
  const sortedBucketKeys = Array.from(buckets.keys()).sort();
  
  const bucketData = sortedBucketKeys.map(bucketKey => {
    const bucket = buckets.get(bucketKey);
    
    // Calculate average tokens for this bucket
    const totalTokens = bucket.commands.reduce((sum, cmd) => sum + (cmd.estimated_tokens || 0), 0);
    const avgTokens = bucket.commands.length > 0 ? totalTokens / bucket.commands.length : 0;
    
    // Parse bucket timestamp for display
    let displayTime;
    if (useHourlyInterval) {
      const [year, month, day, hour] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day, hour);
    } else {
      const [year, month, day] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day);
    }
    
    return {
      time: displayTime,
      avgTokens: avgTokens,
      commandCount: bucket.commands.length,
      timestamp: bucketKey  // Add timestamp for makeDynamicIntervalChart
    };
  });
  
  // Limit to last 60 data points
  const limitedBucketData = bucketData.slice(-60);
  
  // Format labels based on interval type
  const formatLabel = (date) => {
    if (useHourlyInterval) {
      // For 4-hour intervals, show date and time
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        hour: 'numeric',
        hour12: true
      });
    } else {
      // For daily intervals, just show date
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric' 
      });
    }
  };
  
  // Create sparse labels to avoid clutter
  const labels = limitedBucketData.map(d => formatLabel(d.time));
  const sparseLabels = labels.map((label, index) => {
    if (labels.length <= 15) {
      // Show all labels if we have few data points
      return label;
    } else {
      // For many data points, show every Nth label
      const step = Math.ceil(labels.length / 15);
      if (index % step === 0 || index === labels.length - 1) {
        return label;
      }
      return '';
    }
  });
  
  // Extract average token lengths
  const avgTokens = limitedBucketData.map(d => d.avgTokens);
  
  // Find min/max for Y-axis
  const allTokens = avgTokens.filter(v => v !== null && v !== 0);
  const minTokens = Math.min(...allTokens);
  const maxTokens = Math.max(...allTokens);
  
  // Y-axis range with padding
  let yMin = 0;
  let yMax = 50; // Default max
  
  if (allTokens.length > 0) {
    const range = maxTokens - minTokens;
    if (range < 10) {
      // Small range - use tighter bounds
      yMin = Math.max(0, minTokens - 5);
      yMax = maxTokens + 5;
    } else {
      // Larger range - use percentage padding
      const padding = range * 0.2;
      yMin = Math.max(0, minTokens - padding);
      yMax = maxTokens + padding;
    }
  }
  
  // Create the Command Length chart
  chartInstances.commandLength = makeDynamicIntervalChart({
    canvasId: 'command-length-chart',
    labels: sparseLabels,
    datasets: [
      { 
        label: 'avg. tokens/cmd', 
        data: avgTokens,
        ...withColor('blue'), 
        tension: 0.3, 
        pointRadius: 2, 
        pointHoverRadius: 5
      }
    ],
    yScales: {
      y: {
        type: 'linear',
        position: 'left',
        beginAtZero: false,
        min: yMin,
        max: yMax,
        ticks: { precision: 0 },
        title: { display: true, text: 'Average Tokens' }
      }
    },
    tooltipExtra: {
      afterTitle(ctx) {
        const d = limitedBucketData[ctx[0].dataIndex];
        return [
          `Commands in period: ${d.commandCount}`,
          `Average tokens: ${d.avgTokens.toFixed(1)}`
        ];
      },
      label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}`
    },
    optionOverrides: {
      plugins: { legend: { display: false } }
    },
    limitedBucketData: limitedBucketData,
    useHourlyInterval: useHourlyInterval
  });
}

// Create tool usage trends chart - DYNAMIC TIME RANGE
// Note: This chart adapts to show data from first to last project activity,
// making it suitable for viewing historical data from inactive projects
function createToolTrendsChart(statistics) {
  // Get command details sorted by timestamp
  const commandDetails = statistics.user_interactions.command_details || [];
  const nonInterruptionCommands = commandDetails.filter(cmd => !cmd.is_interruption);
    
  if (nonInterruptionCommands.length === 0) {
    // Hide chart if no data
    document.getElementById('tool-trends-chart').parentElement.style.display = 'none';
    return;
  }
    
  // Sort by timestamp
  const sortedCommands = nonInterruptionCommands.sort((a, b) => 
    new Date(a.timestamp) - new Date(b.timestamp)
  );
    
  // Count tool usage across all commands to find top tools
  const toolCounts = {};
  sortedCommands.forEach(cmd => {
    if (cmd.tool_names && Array.isArray(cmd.tool_names)) {
      cmd.tool_names.forEach(toolName => {
        toolCounts[toolName] = (toolCounts[toolName] || 0) + 1;
      });
    }
  });
    
  // Get top tools excluding "Unknown"
  const topTools = Object.entries(toolCounts)
    .filter(([toolName]) => toolName !== 'Unknown')
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([toolName]) => toolName);
    
  // Get time range
  const startTime = new Date(sortedCommands[0].timestamp);
  const endTime = new Date(sortedCommands[sortedCommands.length - 1].timestamp);
  const totalDuration = endTime - startTime;
  const totalDays = totalDuration / (24 * 60 * 60 * 1000);
    
  // Determine interval: 4 hours if < 10 days, otherwise daily
  const useHourlyInterval = totalDays < 10;
  const intervalMs = useHourlyInterval ? 4 * 60 * 60 * 1000 : 24 * 60 * 60 * 1000;
    
  // Create buckets
  const buckets = new Map();
    
  // Helper to get bucket key
  const getBucketKey = (date) => {
    if (useHourlyInterval) {
      const hours = Math.floor(date.getHours() / 4) * 4;
      date.setHours(hours, 0, 0, 0);
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
    } else {
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    }
  };
    
  // Group commands into buckets
  sortedCommands.forEach(cmd => {
    const cmdDate = new Date(cmd.timestamp);
    const bucketKey = getBucketKey(cmdDate);
        
    if (!buckets.has(bucketKey)) {
      buckets.set(bucketKey, {
        commands: [],
        timestamp: bucketKey
      });
    }
        
    buckets.get(bucketKey).commands.push(cmd);
  });
    
  // Sort bucket keys and calculate cumulative averages per tool
  const sortedBucketKeys = Array.from(buckets.keys()).sort();
    
  // Track cumulative counts
  let totalCommands = 0;
  const cumulativeToolCounts = {};
  topTools.forEach(tool => {
    cumulativeToolCounts[tool] = 0;
  });
    
  const bucketData = sortedBucketKeys.map(bucketKey => {
    const bucket = buckets.get(bucketKey);
        
    // Update cumulative counts
    bucket.commands.forEach(cmd => {
      totalCommands++;
      if (cmd.tool_names && Array.isArray(cmd.tool_names)) {
        cmd.tool_names.forEach(toolName => {
          if (cumulativeToolCounts.hasOwnProperty(toolName)) {
            cumulativeToolCounts[toolName]++;
          }
        });
      }
    });
        
    // Calculate cumulative average for each tool
    const toolData = {};
    topTools.forEach(tool => {
      toolData[tool] = totalCommands > 0 ? cumulativeToolCounts[tool] / totalCommands : 0;
    });
        
    // Parse bucket timestamp
    let displayTime;
    if (useHourlyInterval) {
      const [year, month, day, hour] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day, hour);
    } else {
      const [year, month, day] = bucketKey.split('-').map(Number);
      displayTime = new Date(year, month - 1, day);
    }
        
    return {
      timestamp: displayTime,
      toolData: toolData,
      totalCommands: totalCommands,
      commandsInPeriod: bucket.commands.length
    };
  });
    
  // Limit to last 60 data points
  const limitedBucketData = bucketData.slice(-60);
    
  // Format labels
  const formatLabel = (date) => {
    if (useHourlyInterval) {
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric',
        hour: 'numeric',
        hour12: true
      });
    } else {
      return date.toLocaleDateString('en-US', { 
        month: 'short', 
        day: 'numeric' 
      });
    }
  };
    
  // Create labels array
  const labels = limitedBucketData.map(d => formatLabel(d.timestamp));
    
  // Create sparse labels to avoid clutter
  const sparseLabels = labels.map((label, index) => {
    if (labels.length <= 15) {
      return label;
    } else {
      const step = Math.ceil(labels.length / 15);
      if (index % step === 0 || index === labels.length - 1) {
        return label;
      }
      return '';
    }
  });
    
  // Define colors for each tool
  const toolColors = {
    'Read': '#667eea',
    'Edit': '#48bb78',
    'Write': '#ed8936',
    'Bash': '#e53e3e',
    'Grep': '#9f7aea',
    'Task': '#38b2ac',
    'MultiEdit': '#f687b3',
    'TodoWrite': '#fc8181',
    'LS': '#63b3ed',
    'Glob': '#fbd38d'
  };
    
  // Create datasets for each tool
  const datasets = topTools.map(toolName => {
    const data = limitedBucketData.map(bucket => bucket.toolData[toolName]);
        
    return {
      label: toolName,
      data: data,
      borderColor: toolColors[toolName] || '#718096',
      backgroundColor: 'transparent',
      tension: 0.3,
      pointRadius: 2,
      pointHoverRadius: 5,
      borderWidth: 2
    };
  });
    
  // Create the Tool Usage Trends chart
  chartInstances.toolTrends = makeDynamicIntervalChart({
    canvasId: 'tool-trends-chart',
    labels:   sparseLabels,
    datasets,               // your pre-built per-tool dataset array
    yScales: {
      y: {
        type: 'linear', position: 'left', beginAtZero: true,
        ticks: { callback: v => v.toFixed(2) },
        title: { display: true, text: 'Average Uses per Command' }
      }
    },
    tooltipExtra: {
      afterTitle(ctx) {
        const b = limitedBucketData[ctx[0].dataIndex];
        return [
          `Total commands: ${b.totalCommands}`,
          `Commands in period: ${b.commandsInPeriod}`
        ];
      },
      label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(3)} / cmd`
    },
    limitedBucketData: limitedBucketData,
    useHourlyInterval: useHourlyInterval
  });
}

// Create interruption rate trend chart
function createInterruptionRateChart(statistics) {
  const interactionDetails = statistics.user_interactions?.command_details || [];
  const nonInterruptionCommands = interactionDetails.filter(cmd => !cmd.is_interruption && cmd.timestamp);
  
  if (nonInterruptionCommands.length > 0) {
    // Sort by timestamp
    const sortedCommands = nonInterruptionCommands.sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );
    
    // Get time range
    const startTime = new Date(sortedCommands[0].timestamp);
    const endTime = new Date(sortedCommands[sortedCommands.length - 1].timestamp);
    const totalDuration = endTime - startTime;
    const totalDays = totalDuration / (24 * 60 * 60 * 1000);
    
    // Determine interval: 4 hours if < 10 days, otherwise daily
    const useHourlyInterval = totalDays < 10;
    
    // Create time buckets
    const buckets = new Map();
    
    // Helper to get bucket key
    const getBucketKey = (date) => {
      if (useHourlyInterval) {
        const hours = Math.floor(date.getHours() / 4) * 4;
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
      } else {
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
      }
    };
    
    // Group commands into buckets
    sortedCommands.forEach(cmd => {
      const cmdDate = new Date(cmd.timestamp);
      const bucketKey = getBucketKey(cmdDate);
      
      if (!buckets.has(bucketKey)) {
        buckets.set(bucketKey, {
          commands: 0,
          interrupted: 0
        });
      }
      
      buckets.get(bucketKey).commands++;
      if (cmd.followed_by_interruption) {
        buckets.get(bucketKey).interrupted++;
      }
    });
    
    // Sort bucket keys and calculate data
    const sortedBucketKeys = Array.from(buckets.keys()).sort();
    const bucketData = sortedBucketKeys.map(bucketKey => {
      const bucket = buckets.get(bucketKey);
      return {
        timestamp: bucketKey,
        commands: bucket.commands,
        interrupted: bucket.interrupted,
        rate: bucket.commands > 0 ? (bucket.interrupted / bucket.commands) * 100 : 0
      };
    });
    
    // Limit to last 60 data points
    const limitedBucketData = bucketData.slice(-60);
    
    // Format labels
    const formatLabel = (timestamp) => {
      let date;
      if (useHourlyInterval) {
        const [year, month, day, hour] = timestamp.split('-').map(Number);
        date = new Date(year, month - 1, day, hour);
        return date.toLocaleDateString('en-US', { 
          month: 'short', 
          day: 'numeric',
          hour: 'numeric',
          hour12: true
        });
      } else {
        const [year, month, day] = timestamp.split('-').map(Number);
        date = new Date(year, month - 1, day);
        return date.toLocaleDateString('en-US', { 
          month: 'short', 
          day: 'numeric' 
        });
      }
    };
    
    // Create arrays for chart
    const labels = limitedBucketData.map(d => formatLabel(d.timestamp));
    const interruptionRates = limitedBucketData.map(d => d.rate);
    const userCommands = limitedBucketData.map(d => d.commands);
    const interruptedCommands = limitedBucketData.map(d => d.interrupted);
    
    // Create sparse labels
    const sparseLabels = labels.map((label, index) => {
      if (labels.length <= 15) {
        return label;
      } else {
        const step = Math.ceil(labels.length / 15);
        if (index % step === 0 || index === labels.length - 1) {
          return label;
        }
        return '';
      }
    });
    
    chartInstances.interruptionRate = makeDynamicIntervalChart({
      canvasId: 'interruption-rate-trend-chart',
      labels:   sparseLabels,
      datasets: [
        { label: 'Interruption Rate (%)', data: interruptionRates,
          ...withColor('red'),    tension: 0.1, yAxisID: 'y-rate' },
        { label: 'User Commands', data: userCommands,
          ...withColor('blue'),   tension: 0.1, yAxisID: 'y-count', hidden: true },
        { label: 'Interrupted Commands', data: interruptedCommands,
          ...withColor('orange'), tension: 0.1, yAxisID: 'y-count', hidden: true }
      ],
      yScales: {
        'y-rate': {
          type: 'linear', position: 'left', beginAtZero: true,
          max: Math.max(100, Math.ceil(Math.max(...interruptionRates) * 1.1)),
          ticks: { callback: v => v + '%' },
          title: { display: true, text: 'Interruption Rate (%)' }
        },
        'y-count': {
          type: 'linear', position: 'right', beginAtZero: true,
          grid: { drawOnChartArea: false },
          title: { display: true, text: 'Command Count' }
        }
      },
      tooltipExtra: {
        afterLabel(ctx) {
          if (ctx.datasetIndex !== 0) {return '';}
          const i = ctx.dataIndex;
          return `${interruptedCommands[i]} of ${userCommands[i]} commands interrupted`;
        }
      },
      limitedBucketData: limitedBucketData,
      useHourlyInterval: useHourlyInterval
    });
  } else {
    // Hide the chart container if no data
    const chartElement = document.getElementById('interruption-rate-trend-chart');
    if (chartElement && chartElement.parentElement) {
      chartElement.parentElement.style.display = 'none';
    }
  }
}

// Create error rate chart
function createErrorRateChart(statistics) {
  const assistantDetails = statistics.errors.assistant_details || [];
  const assistantMessages = assistantDetails.filter(msg => msg.timestamp);
  
  if (assistantMessages.length > 0) {
    // Get time range
    const sortedMessages = assistantMessages.sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );
    const startTime = new Date(sortedMessages[0].timestamp);
    const endTime = new Date(sortedMessages[sortedMessages.length - 1].timestamp);
    const totalDuration = endTime - startTime;
    const totalDays = totalDuration / (24 * 60 * 60 * 1000);
    
    // Determine interval: 4 hours if < 10 days, otherwise daily
    const useHourlyInterval = totalDays < 10;
    
    // Create time buckets
    const buckets = new Map();
    
    // Helper to get bucket key
    const getBucketKey = (date) => {
      if (useHourlyInterval) {
        const hours = Math.floor(date.getHours() / 4) * 4;
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
      } else {
        return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
      }
    };
    
    // Group messages into buckets
    sortedMessages.forEach(msg => {
      const msgDate = new Date(msg.timestamp);
      const bucketKey = getBucketKey(msgDate);
      
      if (!buckets.has(bucketKey)) {
        buckets.set(bucketKey, {
          assistantMessages: 0,
          errors: 0
        });
      }
      
      buckets.get(bucketKey).assistantMessages++;
      if (msg.is_error) {
        buckets.get(bucketKey).errors++;
      }
    });
    
    // Sort bucket keys and calculate data
    const sortedBucketKeys = Array.from(buckets.keys()).sort();
    const bucketData = sortedBucketKeys.map(bucketKey => {
      const bucket = buckets.get(bucketKey);
      return {
        timestamp: bucketKey,
        assistantMessages: bucket.assistantMessages,
        errors: bucket.errors,
        rate: bucket.assistantMessages > 0 ? (bucket.errors / bucket.assistantMessages) * 100 : 0
      };
    });
    
    // Limit to last 60 data points
    const limitedBucketData = bucketData.slice(-60);
    
    // Format labels
    const formatLabel = (timestamp) => {
      let date;
      if (useHourlyInterval) {
        const [year, month, day, hour] = timestamp.split('-').map(Number);
        date = new Date(year, month - 1, day, hour);
        return date.toLocaleDateString('en-US', { 
          month: 'short', 
          day: 'numeric',
          hour: 'numeric',
          hour12: true
        });
      } else {
        const [year, month, day] = timestamp.split('-').map(Number);
        date = new Date(year, month - 1, day);
        return date.toLocaleDateString('en-US', { 
          month: 'short', 
          day: 'numeric' 
        });
      }
    };
    
    // Create arrays for chart
    const labels = limitedBucketData.map(d => formatLabel(d.timestamp));
    const errorRates = limitedBucketData.map(d => d.rate);
    const errorCounts = limitedBucketData.map(d => d.errors);
    const assistantCounts = limitedBucketData.map(d => d.assistantMessages);
    
    // Create sparse labels
    const sparseLabels = labels.map((label, index) => {
      if (labels.length <= 15) {
        return label;
      } else {
        const step = Math.ceil(labels.length / 15);
        if (index % step === 0 || index === labels.length - 1) {
          return label;
        }
        return '';
      }
    });
    
    chartInstances.errorRate = makeDynamicIntervalChart({
      canvasId: 'error-rate-trend-chart',
      labels: sparseLabels,
      datasets: [
        { 
          label: 'Error Rate (%)', 
          data: errorRates,
          ...withColor('red'), 
          tension: 0.1, 
          yAxisID: 'y-rate' 
        },
        { 
          label: 'Errors', 
          data: errorCounts,
          ...withColor('orange'), 
          tension: 0.1, 
          yAxisID: 'y-count', 
          hidden: true 
        },
        { 
          label: 'Assistant Messages', 
          data: assistantCounts,
          ...withColor('blue'), 
          tension: 0.1, 
          yAxisID: 'y-count', 
          hidden: true 
        }
      ],
      yScales: {
        'y-rate': {
          type: 'linear', 
          position: 'left', 
          beginAtZero: true,
          max: Math.max(10, Math.ceil(Math.max(...errorRates) * 1.1)),
          ticks: { callback: v => v.toFixed(1) + '%' },
          title: { display: true, text: 'Error Rate (%)' }
        },
        'y-count': {
          type: 'linear', 
          position: 'right', 
          beginAtZero: true,
          grid: { drawOnChartArea: false },
          title: { display: true, text: 'Message Count' }
        }
      },
      tooltipExtra: {
        afterLabel(ctx) {
          if (ctx.datasetIndex !== 0) {return '';}
          const i = ctx.dataIndex;
          return `${errorCounts[i]} of ${assistantCounts[i]} assistant messages had errors`;
        }
      },
      limitedBucketData: limitedBucketData,
      useHourlyInterval: useHourlyInterval
    });
  } else {
    // Hide the chart container if no data
    const chartElement = document.getElementById('error-rate-trend-chart');
    if (chartElement && chartElement.parentElement) {
      chartElement.parentElement.style.display = 'none';
    }
  }
}

// Initialize date range pickers for 30-day charts
function initializeDatePickers(statistics) {
  // Get min/max dates from the data
  const dateRange = statistics.overview.date_range;
  let minDate = null;
  let maxDate = new Date();
  
  if (dateRange && dateRange.start) {
    minDate = new Date(dateRange.start);
  }
  
  // Initialize token chart date picker
  tokenDatePicker = new DateRangePicker({
    containerId: 'token-date-picker',
    minDate: minDate,
    maxDate: maxDate,
    defaultDays: 30,
    maxDays: window.maxDateRangeDays || 30,
    onRangeChange: (range) => {
      renderTokenChart(range.startDate, range.endDate);
    }
  });
  
  // Initialize cost chart date picker
  costDatePicker = new DateRangePicker({
    containerId: 'cost-date-picker',
    minDate: minDate,
    maxDate: maxDate,
    defaultDays: 30,
    maxDays: window.maxDateRangeDays || 30,
    onRangeChange: (range) => {
      renderCostChart(range.startDate, range.endDate);
    }
  });
}

// Render token usage chart with date range support
function renderTokenChart(startDate, endDate) {
  if (!fullStatistics || !fullStatistics.daily_stats) {return;}
  
  const dailyStats = fullStatistics.daily_stats;
  const allDates = Object.keys(dailyStats).sort();
  
  // If no date range specified, use last 30 days
  if (!startDate || !endDate) {
    const today = new Date();
    endDate = today.toISOString().split('T')[0];
    const thirtyDaysAgo = new Date(today);
    thirtyDaysAgo.setDate(today.getDate() - 29);
    startDate = thirtyDaysAgo.toISOString().split('T')[0];
  }
  
  // Filter data to the selected range
  const filteredDates = allDates.filter(date => date >= startDate && date <= endDate);
  
  // Prepare data for the chart
  const labels = [];
  const inputTokens = [];
  const outputTokens = [];
  const cacheCreationTokens = [];
  const cacheReadTokens = [];
  
  filteredDates.forEach(date => {
    const data = dailyStats[date];
    // Parse the date string directly to avoid timezone issues
    const [year, month, day] = date.split('-');
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[parseInt(month) - 1];
    labels.push(`${monthName} ${parseInt(day)}`);
    
    const tokens = data.tokens || {};
    inputTokens.push(tokens.input || 0);
    outputTokens.push(tokens.output || 0);
    cacheCreationTokens.push(tokens.cache_creation || 0);
    cacheReadTokens.push(tokens.cache_read || 0);
  });
  
  // Destroy existing chart if it exists
  if (chartInstances.tokens) {
    chartInstances.tokens.destroy();
  }
  
  // Create the token usage chart
  chartInstances.tokens = new Chart(document.getElementById('tokens-chart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {
          label: 'Input Tokens',
          data: inputTokens,
          backgroundColor: '#667eea',
          stack: 'tokens'
        },
        {
          label: 'Output Tokens',
          data: outputTokens,
          backgroundColor: '#764ba2',
          stack: 'tokens'
        },
        {
          label: 'Cache Creation',
          data: cacheCreationTokens,
          backgroundColor: '#48bb78',
          stack: 'tokens',
          hidden: true  // Hide by default
        },
        {
          label: 'Cache Read',
          data: cacheReadTokens,
          backgroundColor: '#38b2ac',
          stack: 'tokens',
          hidden: true  // Hide by default
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          stacked: true,
          ticks: {
            maxRotation: 45,
            minRotation: 45
          }
        },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: {
            callback: function(value) {
              if (value >= 1000000) {
                return (value / 1000000).toFixed(1) + 'M';
              } else if (value >= 1000) {
                return (value / 1000).toFixed(0) + 'K';
              }
              return value;
            }
          },
          title: {
            display: true,
            text: 'Tokens'
          }
        }
      },
      plugins: {
        legend: {
          position: 'bottom'
        },
        tooltip: {
          callbacks: {
            title: function(context) {
              const index = context[0].dataIndex;
              const date = filteredDates[index];
              const [year, month, day] = date.split('-');
              const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 
                'July', 'August', 'September', 'October', 'November', 'December'];
              const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
              // Create date at noon to avoid timezone issues
              const dateObj = new Date(parseInt(year), parseInt(month) - 1, parseInt(day), 12);
              const dayName = dayNames[dateObj.getDay()];
              const monthName = monthNames[parseInt(month) - 1];
              return `${dayName}, ${monthName} ${parseInt(day)}, ${year}`;
            },
            label: function(context) {
              const value = context.parsed.y;
              const label = context.dataset.label;
              return `${label}: ${value.toLocaleString()}`;
            },
            footer: function(tooltipItems) {
              let total = 0;
              tooltipItems.forEach(item => {
                total += item.parsed.y;
              });
              return `Total: ${total.toLocaleString()}`;
            }
          }
        }
      }
    }
  });
}

// Render cost chart with date range support
function renderCostChart(startDate, endDate) {
  if (!fullStatistics || !fullStatistics.daily_stats) {return;}
  
  // Check if pricing is available
  const pricingError = window.PricingUtils ? window.PricingUtils.getPricingError() : null;
  const chartContainer = document.getElementById('daily-cost-chart');
  
  if (pricingError && chartContainer) {
    // Show error message instead of chart
    const chartParent = chartContainer.parentElement;
    chartParent.innerHTML = `
      <h2>Daily Cost Breakdown</h2>
      <div style="text-align: center; padding: 3rem; color: #666;">
        <p style="font-size: 1.1rem; margin-bottom: 1rem;">${pricingError}</p>
        <button onclick="PricingUtils.refreshPricing().then(() => refreshData())" 
                style="padding: 0.5rem 1rem; background: #667eea; color: white; border: none; 
                       border-radius: 4px; cursor: pointer;">
          Try Again
        </button>
      </div>
    `;
    return;
  }
  
  const dailyStats = fullStatistics.daily_stats;
  const allDates = Object.keys(dailyStats).sort();
  
  // If no date range specified, use last 30 days
  if (!startDate || !endDate) {
    const today = new Date();
    endDate = today.toISOString().split('T')[0];
    const thirtyDaysAgo = new Date(today);
    thirtyDaysAgo.setDate(today.getDate() - 29);
    startDate = thirtyDaysAgo.toISOString().split('T')[0];
  }
  
  // Filter data to the selected range
  const filteredDates = allDates.filter(date => date >= startDate && date <= endDate);
  
  // Make sure the chart container is visible
  document.getElementById('daily-cost-chart').parentElement.style.display = '';
  
  // Prepare data for the chart
  const labels = [];
  const inputCosts = [];
  const outputCosts = [];
  const cacheCosts = [];
  
  filteredDates.forEach(date => {
    const data = dailyStats[date];
    // Parse the date string directly to avoid timezone issues
    const [year, month, day] = date.split('-');
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[parseInt(month) - 1];
    labels.push(`${monthName} ${parseInt(day)}`);
    
    const cost = data.cost || { total: 0 };
    
    // Calculate component costs
    let dayInputCost = 0;
    let dayOutputCost = 0;
    let dayCacheCost = 0;
    
    if (cost.by_model) {
      Object.values(cost.by_model).forEach(modelCost => {
        dayInputCost += modelCost.input_cost || 0;
        dayOutputCost += modelCost.output_cost || 0;
        dayCacheCost += (modelCost.cache_creation_cost || 0) + (modelCost.cache_read_cost || 0);
      });
    }
    
    inputCosts.push(dayInputCost);
    outputCosts.push(dayOutputCost);
    cacheCosts.push(dayCacheCost);
  });
  
  // Calculate total project cost from ALL data (not just selected range)
  let projectTotalCost = 0;
  allDates.forEach(date => {
    const data = dailyStats[date];
    projectTotalCost += (data.cost && data.cost.total) || 0;
  });
  
  // Update the total cost text
  // const totalCostElement = document.getElementById('total-cost-text');
  // if (totalCostElement) {
  //   const finalTotal = projectTotalCost;
  //   if (finalTotal < 0.01) {
  //     totalCostElement.textContent = `Total pay-as-you-go cost = $${finalTotal.toFixed(4)}`;
  //   } else if (finalTotal < 1) {
  //     totalCostElement.textContent = `Total pay-as-you-go cost = $${finalTotal.toFixed(3)}`;
  //   } else {
  //     totalCostElement.textContent = `Total pay-as-you-go cost = $${finalTotal.toFixed(2)}`;
  //   }
  // }
  
  // Destroy existing chart if it exists
  if (chartInstances.dailyCost) {
    chartInstances.dailyCost.destroy();
  }
  
  // Create the cost chart
  chartInstances.dailyCost = new Chart(document.getElementById('daily-cost-chart'), {
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
      layout: {
        padding: {
          bottom: 20,
          left: 10,
          right: 10
        }
      },
      scales: {
        x: {
          stacked: true,
          ticks: {
            maxRotation: 45,
            minRotation: 45
          }
        },
        y: {
          stacked: true,
          position: 'left',
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
          },
          title: {
            display: true,
            text: 'Daily Cost (USD)'
          }
        }
      },
      plugins: {
        legend: {
          position: 'top'
        },
        tooltip: {
          callbacks: {
            title: function(context) {
              const index = context[0].dataIndex;
              const date = filteredDates[index];
              const [year, month, day] = date.split('-').map(Number);
              const localDate = new Date(year, month - 1, day);
              return localDate.toLocaleDateString('en-US', {
                weekday: 'long',
                year: 'numeric',
                month: 'long',
                day: 'numeric'
              });
            },
            afterTitle: function(context) {
              const index = context[0].dataIndex;
              const date = filteredDates[index];
              const data = dailyStats[date];
              return `Messages: ${data.messages}`;
            },
            label: function(context) {
              const value = context.parsed.y;
              const label = context.dataset.label;
              if (value < 0.01) {
                return `${label}: $${value.toFixed(4)}`;
              } else if (value < 1) {
                return `${label}: $${value.toFixed(3)}`;
              } else {
                return `${label}: $${value.toFixed(2)}`;
              }
            },
            footer: function(tooltipItems) {
              let total = 0;
              tooltipItems.forEach(item => {
                total += item.parsed.y;
              });
              if (total < 0.01) {
                return `Total: $${total.toFixed(4)}`;
              } else if (total < 1) {
                return `Total: $${total.toFixed(3)}`;
              } else {
                return `Total: $${total.toFixed(2)}`;
              }
            }
          }
        }
      }
    }
  });
}
// Step-Length Metrics Charts for Sniffly
// This module contains the implementation for step-length visualization

// Create step-length metrics charts
function createStepLengthCharts(statistics) {
  const userInteractions = statistics.user_interactions || {};
  const stepSequences = userInteractions.step_sequences || [];
  const distribution = userInteractions.step_length_distribution || {};
  
  // 1. Step-Length Distribution Chart (Bar Chart)
  if (Object.keys(distribution).length > 0) {
    // Sort keys for proper display order
    const sortedKeys = Object.keys(distribution).sort((a, b) => {
      // Handle "10+" specially
      if (a === "10+") return 1;
      if (b === "10+") return -1;
      return parseInt(a) - parseInt(b);
    });
    
    const distributionData = sortedKeys.map(key => distribution[key]);
    
    chartInstances.stepLengthDist = new Chart(document.getElementById('step-length-distribution-chart'), {
      type: 'bar',
      data: {
        labels: sortedKeys.map(key => key === "10+" ? "10+" : `${key}`),
        datasets: [{
          label: 'Number of Sequences',
          data: distributionData,
          backgroundColor: '#48bb78', // Green color for positive metric
          borderColor: '#38a169',
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: false
          },
          legend: {
            display: false
          },
          tooltip: {
            callbacks: {
              label: function(context) {
                const value = context.raw;
                const total = distributionData.reduce((a, b) => a + b, 0);
                const percentage = ((value / total) * 100).toFixed(1);
                return `${value} sequences (${percentage}%)`;
              }
            }
          }
        },
        scales: {
          x: {
            title: {
              display: true,
              text: 'Consecutive Tool Uses'
            }
          },
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Number of Sequences'
            },
            ticks: {
              stepSize: 1
            }
          }
        }
      }
    });
  } else {
    // Hide chart if no data
    const chartElement = document.getElementById('step-length-distribution-chart');
    if (chartElement && chartElement.parentElement) {
      chartElement.parentElement.style.display = 'none';
    }
  }
  
  // 2. Step-Length Metrics Over Time (Line Chart)
  if (stepSequences.length > 0) {
    // Sort sequences by timestamp
    const sortedSequences = stepSequences.filter(seq => seq.timestamp).sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );
    
    if (sortedSequences.length > 0) {
      // Get time range
      const startTime = new Date(sortedSequences[0].timestamp);
      const endTime = new Date(sortedSequences[sortedSequences.length - 1].timestamp);
      const totalDuration = endTime - startTime;
      const totalDays = totalDuration / (24 * 60 * 60 * 1000);
      
      // Determine interval
      const useHourlyInterval = totalDays < 10;
      
      // Create time buckets
      const buckets = new Map();
      
      // Helper to get bucket key
      const getBucketKey = (date) => {
        if (useHourlyInterval) {
          const hours = Math.floor(date.getHours() / 4) * 4;
          return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}-${String(hours).padStart(2, '0')}`;
        } else {
          return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
        }
      };
      
      // Group sequences into buckets
      sortedSequences.forEach(seq => {
        const seqDate = new Date(seq.timestamp);
        const bucketKey = getBucketKey(seqDate);
        
        if (!buckets.has(bucketKey)) {
          buckets.set(bucketKey, {
            sequences: [],
            totalLength: 0,
            count: 0
          });
        }
        
        const bucket = buckets.get(bucketKey);
        bucket.sequences.push(seq);
        bucket.totalLength += seq.length;
        bucket.count++;
      });
      
      // Sort bucket keys and calculate data
      const sortedBucketKeys = Array.from(buckets.keys()).sort();
      const bucketData = sortedBucketKeys.map(bucketKey => {
        const bucket = buckets.get(bucketKey);
        return {
          timestamp: bucketKey,
          avgLength: bucket.totalLength / bucket.count,
          maxLength: Math.max(...bucket.sequences.map(s => s.length)),
          count: bucket.count
        };
      });
      
      // Limit to last 60 data points
      const limitedBucketData = bucketData.slice(-60);
      
      // Format labels
      const formatLabel = (timestamp) => {
        let date;
        if (useHourlyInterval) {
          const [year, month, day, hour] = timestamp.split('-').map(Number);
          date = new Date(year, month - 1, day, hour);
          return date.toLocaleDateString('en-US', { 
            month: 'short', 
            day: 'numeric',
            hour: 'numeric',
            hour12: true
          });
        } else {
          const [year, month, day] = timestamp.split('-').map(Number);
          date = new Date(year, month - 1, day);
          return date.toLocaleDateString('en-US', { 
            month: 'short', 
            day: 'numeric' 
          });
        }
      };
      
      // Create arrays for chart
      const labels = limitedBucketData.map(d => formatLabel(d.timestamp));
      const avgStepLengths = limitedBucketData.map(d => d.avgLength.toFixed(2));
      const maxStepLengths = limitedBucketData.map(d => d.maxLength);
      
      // Create sparse labels to avoid clutter
      const sparseLabels = labels.map((label, index) => {
        if (labels.length <= 15) {
          return label;
        } else {
          const step = Math.ceil(labels.length / 15);
          if (index % step === 0 || index === labels.length - 1) {
            return label;
          }
          return '';
        }
      });
      
      chartInstances.stepLengthTrend = makeDynamicIntervalChart({
        canvasId: 'step-length-trend-chart',
        labels: sparseLabels,
        datasets: [
          { 
            label: 'Average Step Length', 
            data: avgStepLengths,
            borderColor: '#48bb78',
            backgroundColor: 'rgba(72, 187, 120, 0.1)',
            tension: 0.1, 
            yAxisID: 'y-length' 
          },
          { 
            label: 'Max Step Length', 
            data: maxStepLengths,
            ...withColor('blue'), 
            tension: 0.1, 
            yAxisID: 'y-length',
            hidden: true 
          }
        ],
        yScales: {
          'y-length': {
            type: 'linear',
            position: 'left',
            beginAtZero: true,
            title: {
              display: true,
              text: 'Step Length'
            }
          }
        },
        tooltipExtra: {
          afterLabel(ctx) {
            const i = ctx.dataIndex;
            const bucket = limitedBucketData[i];
            if (ctx.datasetIndex === 0) {
              return `${bucket.count} sequences in this period`;
            }
            return '';
          }
        },
        limitedBucketData: limitedBucketData,
        useHourlyInterval: useHourlyInterval
      });
    }
  } else {
    // Hide chart if no data
    const chartElement = document.getElementById('step-length-trend-chart');
    if (chartElement && chartElement.parentElement) {
      chartElement.parentElement.style.display = 'none';
    }
  }
}
