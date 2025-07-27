/**
 * Messages Tab Module
 * Handles all functionality for the Messages tab in the dashboard
 */

// Note: This module expects the following global variables to be defined:
// - allMessages: array of all message objects
// - filteredMessages: array to store filtered results (will be updated by this module)

// Module state variables (these were global in the original implementation)
let currentPage = 1;
let messagesPerPage = DEFAULT_MESSAGES_PER_PAGE;
let sortColumn = 'timestamp';
let sortDirection = 'desc';

// Initialize filters
function initializeFilters() {
  // Type filter - use all types from statistics, not just from loaded messages
  const typeFilter = document.getElementById('type-filter');
  
  // Clear existing options except "All Types"
  while (typeFilter.options.length > 1) {
    typeFilter.remove(1);
  }
  
  if (statistics && statistics.overview && statistics.overview.message_types) {
    // Get all message types from statistics and sort them
    const allTypes = Object.keys(statistics.overview.message_types).sort();
    console.log(`Loading ${allTypes.length} message types from statistics:`, allTypes);
    
    // Add tool_result as a special type if there are any messages with tool results
    const hasToolResults = allMessages.some(m => m.has_tool_result);
    if (hasToolResults && !allTypes.includes('tool_result')) {
      allTypes.push('tool_result');
      allTypes.sort();
    }
    
    allTypes.forEach(type => {
      let displayName = type;
      if (type === 'tool_result') {displayName = 'tool result';}
      else if (type === 'summary') {displayName = 'summary';}
      else if (type === 'compact_summary') {displayName = 'compact summary';}
      
      const count = statistics.overview.message_types[type] || 0;
      const option = new Option(count > 0 ? `${displayName} (${count})` : displayName, type);
      typeFilter.add(option);
    });
  } else {
    // Fallback to old method if statistics not available
    console.log('Statistics not available for types, falling back to message-based type list');
    const types = new Set();
    allMessages.forEach(m => {
      if (m.has_tool_result) {
        types.add('tool_result');
      } else {
        types.add(m.type);
      }
    });
    
    Array.from(types).sort().forEach(type => {
      let displayName = type;
      if (type === 'tool_result') {displayName = 'tool result';}
      else if (type === 'summary') {displayName = 'summary';}
      else if (type === 'compact_summary') {displayName = 'compact summary';}
      const option = new Option(displayName, type);
      typeFilter.add(option);
    });
  }
    
  // Tool filter - use all tools from statistics, not just from loaded messages
  const toolFilter = document.getElementById('tool-filter');
  
  // Clear existing options except "All Tools"
  while (toolFilter.options.length > 1) {
    toolFilter.remove(1);
  }
  
  if (statistics && statistics.tools && statistics.tools.usage_counts) {
    // Get all tool names from statistics and sort them
    const allTools = Object.keys(statistics.tools.usage_counts).sort();
    allTools.forEach(tool => {
      const count = statistics.tools.usage_counts[tool];
      // Show count in parentheses for better context
      const option = new Option(`${tool} (${count})`, tool);
      toolFilter.add(option);
    });
  } else {
    // Fallback to old method if statistics not available
    const tools = new Set();
    allMessages.forEach(m => {
      m.tools.forEach(t => tools.add(t.name));
    });
    const toolArray = Array.from(tools).sort();
    toolArray.forEach(tool => {
      const option = new Option(tool, tool);
      toolFilter.add(option);
    });
  }
    
    
  // Add event listeners
  document.getElementById('type-filter').addEventListener('change', applyFilters);
  document.getElementById('error-filter').addEventListener('change', applyFilters);
  document.getElementById('tool-filter').addEventListener('change', applyFilters);
  document.getElementById('search-input').addEventListener('input', debounce(applyFilters, 300));
  document.getElementById('per-page').addEventListener('change', (e) => {
    messagesPerPage = parseInt(e.target.value);
    currentPage = 1;
    displayMessages();
  });
}

// Apply filters
async function applyFilters() {
  const typeFilter = document.getElementById('type-filter').value;
  const errorFilter = document.getElementById('error-filter').value;
  const toolFilter = document.getElementById('tool-filter').value;
  const searchTerm = document.getElementById('search-input').value.toLowerCase();
    
  // Check if we have any active filters
  const hasActiveFilters = typeFilter || errorFilter || toolFilter || searchTerm;
    
  // If user is searching and we haven't loaded all messages, load them now
  if (searchTerm && !window.messagesFullyLoaded && !window.loadingMoreMessages) {
    console.log('🔍 Search activated - loading all messages...');
    await loadMoreMessages();
  }
    
  // For filters, we need all messages loaded
  if (hasActiveFilters && !window.messagesFullyLoaded && !window.loadingMoreMessages) {
    console.log('🔍 Filters activated - loading all messages...');
    await loadMoreMessages();
  }
    
  filteredMessages = allMessages.filter(message => {
    // Type filter - handle tool_result as a special case
    if (typeFilter) {
      if (typeFilter === 'tool_result') {
        // Show only messages with tool results
        if (!message.has_tool_result) {return false;}
      } else {
        // For other types, check the actual type
        // Task messages can have tool results, so don't exclude them
        if (message.type !== typeFilter) {return false;}
        // Only exclude tool results if it's not a task message
        if (message.type !== 'task' && message.has_tool_result) {return false;}
      }
    }
        
    // Error filter
    if (errorFilter) {
      if (errorFilter === 'errors-only' && !message.error) {return false;}
      if (errorFilter === 'no-errors' && message.error) {return false;}
    }
        
    // Tool filter
    if (toolFilter && !message.tools.some(t => t.name === toolFilter)) {return false;}
        
        
    // Search filter
    if (searchTerm && !message.content.toLowerCase().includes(searchTerm)) {return false;}
        
    return true;
  });
    
  currentPage = 1;
  sortMessages();
  displayMessages();
  updateTypeFilterCounts();
}

// Sort messages
function sortMessages() {
  const columnConfig = {
    'type': (msg) => msg.has_tool_result ? 'tool_result' : msg.type,
    'content': (msg) => msg.content || '',
    'timestamp': (msg) => msg.timestamp || '',
    'model': (msg) => msg.model || '',
    'tokens': (msg) => msg.tokens.input + msg.tokens.output
  };
    
  sortTableData(filteredMessages, sortColumn, sortDirection, columnConfig);
}

// Sort table by column
async function sortTable(column) {
  // Load all messages if sorting and not fully loaded
  if (!window.messagesFullyLoaded && !window.loadingMoreMessages) {
    console.log('📊 Sorting activated - loading all messages...');
    await loadMoreMessages();
  }
  
  if (sortColumn === column) {
    // Toggle direction
    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
  } else {
    // New column, default to appropriate direction
    sortColumn = column;
    sortDirection = column === 'timestamp' ? 'desc' : 'asc';
  }
    
  currentPage = 1;
  sortMessages();
  displayMessages();
  updateSortIndicators();
}

// Update sort indicators
function updateSortIndicators() {
  // Clear all sort classes from message table headers
  const messageTable = document.querySelector('#messages-table');
  if (!messageTable) {return;}
    
  messageTable.querySelectorAll('th[onclick^="sortTable"]').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    // Ensure sortable class is present
    th.classList.add('sortable');
  });
    
  // Find and update the active column
  const headers = messageTable.querySelectorAll('th[onclick^="sortTable"]');
  headers.forEach(th => {
    const onclickAttr = th.getAttribute('onclick');
    const match = onclickAttr.match(/sortTable\('([^']+)'\)/);
    if (match && match[1] === sortColumn) {
      th.classList.add(sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
    }
  });
}

// Display messages
function displayMessages() {
  const start = (currentPage - 1) * messagesPerPage;
  const end = start + messagesPerPage;
  const pageMessages = filteredMessages.slice(start, end);
    
  // Update count to show loaded vs total
  const hasActiveFilters = document.getElementById('type-filter').value ||
                           document.getElementById('error-filter').value ||
                           document.getElementById('tool-filter').value ||
                           document.getElementById('search-input').value;
    
  let countHTML;
    
  if (hasActiveFilters) {
    // When filters are active, show filtered count
    countHTML = `<div style="font-size: 0.9rem; color: #666;"><strong>${filteredMessages.length.toLocaleString()}</strong> messages</div>`;
  } else if (window.totalMessageCount) {
    // No filters and we know the total - always use it
    const totalCount = window.totalMessageCount;
    const loadedCount = allMessages.length;
    countHTML = `<div style="font-size: 0.9rem; color: #666;"><strong>${totalCount.toLocaleString()}</strong> messages</div>`;
  } else {
    // No total count known - show what we have
    countHTML = `<div style="font-size: 0.9rem; color: #666;"><strong>${filteredMessages.length.toLocaleString()}</strong> messages</div>`;
  }
    
  document.getElementById('message-count').innerHTML = countHTML;
    
  // Build table rows
  const tbody = document.getElementById('messages-tbody');
    
  // Check if we're trying to display messages that aren't loaded yet
  if (pageMessages.length === 0 && start < window.totalMessageCount && !window.messagesFullyLoaded) {
    // Show placeholder rows while loading
    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px;">Loading messages...</td></tr>';
        
    // Trigger load of all messages
    if (!window.loadingMoreMessages) {
      loadMoreMessages().then(() => {
        displayMessages(); // Re-display after loading
      });
    }
    return;
  }
    
  tbody.innerHTML = pageMessages.map((message, index) => `
        <tr onclick="showMessageDetail(${start + index})">
            <td>
                <span class="type-badge ${message.type} ${message.has_tool_result ? 'tool-result' : ''}">
                    ${message.type === 'task' && message.has_tool_result ? 'task (tool result)' :
    message.has_tool_result ? 'tool result' : 
      message.type === 'compact_summary' ? 'compact summary' :
        message.type}
                </span>
            </td>
            <td>
                <div class="message-content" title="${escapeHtml(message.content)}">
                    ${message.error ? '<span class="error-indicator">⚠️</span> ' : ''}
                    ${escapeHtml(truncateContent(message.content || '(empty)'))}
                </div>
            </td>
            <td>${formatTimestamp(message.timestamp)}</td>
            <td>
                ${message.model && message.model !== 'N/A' ? 
    formatModelName(message.model) : 
    '<span style="color: #999;">-</span>'}
            </td>
            <td class="tokens">${formatTokens(message.tokens)}</td>
            <td>
                <div class="tools-list">
                    ${message.tools.map(t => 
    `<span class="tool-chip">${t.name}</span>`
  ).join('')}
                </div>
            </td>
        </tr>
    `).join('');
    
  // Update pagination
  displayPagination();
    
  // Update sort indicators on first load
  updateSortIndicators();
}

// Display pagination controls
function displayPagination() {
  // Check if we're in Messages tab and have a total count
  const messagesTab = document.getElementById('messages-tab');
  const isMessagesTab = messagesTab && messagesTab.classList.contains('active');
  let totalItems = filteredMessages.length;
    
  // Use virtual pagination for Messages tab if we know the total
  if (isMessagesTab && window.totalMessageCount && !window.messagesFullyLoaded) {
    // For unloaded messages, calculate based on total count
    // But only for unfiltered view (filters require all data)
    const hasActiveFilters = document.getElementById('type-filter').value ||
                               document.getElementById('error-filter').value ||
                               document.getElementById('tool-filter').value ||
                               document.getElementById('search-input').value;
        
    if (!hasActiveFilters) {
      totalItems = window.totalMessageCount;
    }
  }
    
  const totalPages = Math.ceil(totalItems / messagesPerPage);
    
  // Update pagination visibility
  const paginationElement = document.getElementById('messages-pagination');
  if (paginationElement) {
    paginationElement.style.display = totalItems > messagesPerPage ? 'flex' : 'none';
  }
    
  // Update page numbers
  const pageInput = document.getElementById('messages-page-input');
  const totalPagesSpan = document.getElementById('messages-total-pages');
  if (pageInput) {pageInput.value = currentPage;}
  if (totalPagesSpan) {totalPagesSpan.textContent = totalPages;}
    
  // Update button states
  const prevBtn = document.getElementById('messages-prev-btn');
  const nextBtn = document.getElementById('messages-next-btn');
  if (prevBtn) {prevBtn.disabled = currentPage === 1;}
  if (nextBtn) {nextBtn.disabled = currentPage === totalPages;}
}

// Navigate to page
async function goToPage(page) {
  // Calculate total pages based on virtual or actual data
  const isMessagesTab = document.getElementById('messages-tab').classList.contains('active');
  const hasActiveFilters = document.getElementById('type-filter').value ||
                           document.getElementById('error-filter').value ||
                           document.getElementById('tool-filter').value ||
                           document.getElementById('search-input').value;
    
  let totalItems = filteredMessages.length;
  if (isMessagesTab && window.totalMessageCount && !window.messagesFullyLoaded && !hasActiveFilters) {
    totalItems = window.totalMessageCount;
  }
    
  const totalPages = Math.ceil(totalItems / messagesPerPage);
  if (page < 1) {page = 1;}
  if (page > totalPages) {page = totalPages;}
    
  // Check if we're navigating to a page with unloaded data
  const requestedMessageIndex = (page - 1) * messagesPerPage;
  if (isMessagesTab && requestedMessageIndex >= allMessages.length && !window.messagesFullyLoaded) {
    // Need to load all messages to access this page
    console.log(`📥 Loading all messages to access page ${page}...`);
        
    // Show loading state
    const tbody = document.getElementById('messages-tbody');
    if (tbody) {
      tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px;">Loading messages...</td></tr>';
    }
        
    // Load all messages
    await loadMoreMessages();
  }
    
  currentPage = page;
  displayMessages();
    
  // Check if we need to load more messages
  checkLoadMore();
}

// Check if we need to load more messages
function checkLoadMore() {
  // Only load more if we're on Messages tab and haven't loaded all messages
  if (!window.messagesFullyLoaded && window.totalMessageCount) {
    const totalPages = Math.ceil(window.totalMessageCount / messagesPerPage);
    const loadedPages = Math.ceil(allMessages.length / messagesPerPage);
    const pagesRemaining = loadedPages - currentPage;
        
    // Load more if we're within 5 pages of the end
    if (pagesRemaining <= 5 && !window.loadingMoreMessages) {
      loadMoreMessages();
    }
  }
}

// Load more messages
async function loadMoreMessages() {
  if (window.loadingMoreMessages || window.messagesFullyLoaded) {return;}
    
  window.loadingMoreMessages = true;
  console.log('📥 Loading more messages...');
    
  try {
    // Load all remaining messages
    const response = await fetch('/api/messages');
    if (response.ok) {
      const messages = await response.json();
      const messageArray = Array.isArray(messages) ? messages : messages.messages;
            
      allMessages = messageArray;
      window.allMessages = messageArray;
      window.messagesFullyLoaded = true;
            
      // Re-apply filters to include new messages
      applyFilters();
            
      console.log(`✅ All messages loaded: ${messageArray.length} total`);
    }
  } catch (error) {
    console.error('Failed to load more messages:', error);
  } finally {
    window.loadingMoreMessages = false;
  }
}

// Messages pagination functions
function changeMessagesPage(delta) {
  goToPage(currentPage + delta);
}

async function goToMessagesPage() {
  const pageInput = document.getElementById('messages-page-input');
  if (pageInput) {
    await goToPage(parseInt(pageInput.value) || 1);
  }
}

function updateMessagesPerPage() {
  const select = document.getElementById('messages-per-page');
  if (select) {
    messagesPerPage = parseInt(select.value);
    currentPage = 1;
    displayMessages();
  }
}

// Update type filter counts based on current filtered messages
function updateTypeFilterCounts() {
  const typeFilter = document.getElementById('type-filter');
  if (!typeFilter) return;
  
  const currentValue = typeFilter.value;
  
  // Count message types in filtered results
  const typeCounts = {};
  let toolResultCount = 0;
  
  // Always use filteredMessages to count (even if empty)
  const messagesToCount = filteredMessages;
  
  messagesToCount.forEach(message => {
    // Count tool results (these might also be counted in their original type)
    if (message.has_tool_result) {
      toolResultCount++;
    }
    
    // Count the actual message type
    if (message.type) {
      typeCounts[message.type] = (typeCounts[message.type] || 0) + 1;
    }
  });
  
  // If we have tool results, add them to counts
  if (toolResultCount > 0) {
    typeCounts['tool_result'] = toolResultCount;
  }
  
  // Update dropdown options
  Array.from(typeFilter.options).forEach(option => {
    if (option.value === '') return; // Skip "All Types" option
    
    const type = option.value;
    let displayName = type;
    
    // Format display names
    if (type === 'tool_result') displayName = 'tool result';
    else if (type === 'summary') displayName = 'summary';
    else if (type === 'compact_summary') displayName = 'compact summary';
    
    const count = typeCounts[type] || 0;
    
    // Update option text with new count
    option.text = count > 0 ? `${displayName} (${count})` : displayName;
  });
  
  // Restore the selected value
  typeFilter.value = currentValue;
}

// Show message detail
function showMessageDetail(index) {
  const message = filteredMessages[index];
  const modal = document.getElementById('message-modal');
  const modalTitle = document.getElementById('modal-title');
  const modalBody = document.getElementById('modal-body');
  
  // The index parameter is already the absolute index in filteredMessages
  // (it's passed as start + index from displayMessages)
  const filteredRowNumber = index;
  
  // For navigation, we need the position in the original (unfiltered) allMessages array
  let originalIndex = -1;
  
  // Check if there are no active filters
  const hasActiveFilters = document.getElementById('type-filter').value ||
                           document.getElementById('error-filter').value ||
                           document.getElementById('tool-filter').value ||
                           document.getElementById('search-input').value;
  
  if (!hasActiveFilters) {
    // No filters active, so the filtered row number IS the original index
    originalIndex = filteredRowNumber;
  } else {
    // Filters are active, need to find the message in the original array
    if (message.message_id) {
      originalIndex = allMessages.findIndex(m => m.message_id === message.message_id);
    }
    
    // If not found by message_id or no message_id exists, try to match by multiple fields
    if (originalIndex === -1) {
      originalIndex = allMessages.findIndex(m => 
        m.timestamp === message.timestamp && 
        m.content === message.content &&
        m.type === message.type
      );
    }
  }
  
  // Store current navigation context
  window.messageNavigation = {
    currentIndex: originalIndex,
    totalMessages: allMessages.length,
    filteredIndex: index
  };
    
  modalTitle.textContent = 'Message Details';
  
  // Show navigation for messages
  const modalNav = document.querySelector('.modal-navigation');
  if (modalNav) {
    modalNav.style.display = 'flex';
  }
  
  modalBody.innerHTML = `
        <div class="detail-grid">
            <div class="detail-label">Type:</div>
            <div class="detail-value">
                <span class="type-badge ${message.type} ${message.has_tool_result ? 'tool-result' : ''}">
                    ${message.type === 'task' && message.has_tool_result ? 'task (tool result)' :
    message.has_tool_result ? 'tool result' : 
      message.type === 'compact_summary' ? 'compact summary' :
        message.type}
                </span>
            </div>
            
            <div class="detail-label">Timestamp:</div>
            <div class="detail-value">${new Date(message.timestamp).toLocaleString()}</div>
            
            <div class="detail-label">Session ID:</div>
            <div class="detail-value">
                <code>${message.session_id}</code>
                <button onclick="copyToClipboard('${message.session_id}')">Copy</button>
            </div>
            
            ${message.message_id ? `
                <div class="detail-label">Message ID:</div>
                <div class="detail-value">
                    <code>${message.message_id}</code>
                    <button onclick="copyToClipboard('${message.message_id}')">Copy</button>
                </div>
            ` : ''}
            
            ${message.model !== 'N/A' ? `
                <div class="detail-label">Model:</div>
                <div class="detail-value">${message.model}</div>
            ` : ''}
            
            <div class="detail-label">Tokens:</div>
            <div class="detail-value">
                Input: ${message.tokens.input.toLocaleString()}<br>
                Output: ${message.tokens.output.toLocaleString()}<br>
                ${message.tokens.cache_creation ? `Cache Created: ${message.tokens.cache_creation.toLocaleString()}<br>` : ''}
                ${message.tokens.cache_read ? `Cache Read: ${message.tokens.cache_read.toLocaleString()}<br>` : ''}
            </div>
            
            ${message.cwd ? `
                <div class="detail-label">Working Dir:</div>
                <div class="detail-value"><code>${message.cwd}</code></div>
            ` : ''}
            
            ${message.error ? `
                <div class="detail-label">Error:</div>
                <div class="detail-value error-indicator">This message contains an error</div>
            ` : ''}
        </div>
        
        <h3>Message Content</h3>
        <div class="detail-content">${escapeHtml(message.content || '(empty)')}</div>
        
        ${message.tools.length > 0 ? `
            <h3>Tools Used</h3>
            <div class="detail-tools">
                ${message.tools.map(tool => `
                    <div class="detail-tool">
                        <div class="detail-tool-name">${tool.name}</div>
                        <div class="detail-tool-input">${JSON.stringify(tool.input, null, 2)}</div>
                    </div>
                `).join('')}
            </div>
        ` : ''}
    `;
    
  modal.style.display = 'block';
  
  // Update navigation buttons and position
  updateModalNavigation();
}

// Navigate to previous/next message
async function navigateMessage(direction) {
  if (!window.messageNavigation) return;
  
  const newIndex = window.messageNavigation.currentIndex + direction;
  
  // Check bounds
  if (newIndex < 0 || newIndex >= window.messageNavigation.totalMessages) {
    return;
  }
  
  // Check if we need to load all messages first
  if (!window.messagesFullyLoaded && newIndex >= allMessages.length) {
    console.log('📥 Loading all messages for navigation...');
    await loadMoreMessages();
  }
  
  // Update navigation state
  window.messageNavigation.currentIndex = newIndex;
  
  // Get the message at the new index from allMessages
  const message = allMessages[newIndex];
  
  // Update modal content
  const modalBody = document.getElementById('modal-body');
  modalBody.innerHTML = `
        <div class="detail-grid">
            <div class="detail-label">Type:</div>
            <div class="detail-value">
                <span class="type-badge ${message.type} ${message.has_tool_result ? 'tool-result' : ''}">
                    ${message.type === 'task' && message.has_tool_result ? 'task (tool result)' :
    message.has_tool_result ? 'tool result' : 
      message.type === 'compact_summary' ? 'compact summary' :
        message.type}
                </span>
            </div>
            
            <div class="detail-label">Timestamp:</div>
            <div class="detail-value">${new Date(message.timestamp).toLocaleString()}</div>
            
            <div class="detail-label">Session ID:</div>
            <div class="detail-value">
                <code>${message.session_id}</code>
                <button onclick="copyToClipboard('${message.session_id}')">Copy</button>
            </div>
            
            ${message.message_id ? `
                <div class="detail-label">Message ID:</div>
                <div class="detail-value">
                    <code>${message.message_id}</code>
                    <button onclick="copyToClipboard('${message.message_id}')">Copy</button>
                </div>
            ` : ''}
            
            ${message.model !== 'N/A' ? `
                <div class="detail-label">Model:</div>
                <div class="detail-value">${message.model}</div>
            ` : ''}
            
            <div class="detail-label">Tokens:</div>
            <div class="detail-value">
                Input: ${message.tokens.input.toLocaleString()}<br>
                Output: ${message.tokens.output.toLocaleString()}<br>
                ${message.tokens.cache_creation ? `Cache Created: ${message.tokens.cache_creation.toLocaleString()}<br>` : ''}
                ${message.tokens.cache_read ? `Cache Read: ${message.tokens.cache_read.toLocaleString()}<br>` : ''}
            </div>
            
            ${message.cwd ? `
                <div class="detail-label">Working Dir:</div>
                <div class="detail-value"><code>${message.cwd}</code></div>
            ` : ''}
            
            ${message.error ? `
                <div class="detail-label">Error:</div>
                <div class="detail-value error-indicator">This message contains an error</div>
            ` : ''}
        </div>
        
        <h3>Message Content</h3>
        <div class="detail-content">${escapeHtml(message.content || '(empty)')}</div>
        
        ${message.tools.length > 0 ? `
            <h3>Tools Used</h3>
            <div class="detail-tools">
                ${message.tools.map(tool => `
                    <div class="detail-tool">
                        <div class="detail-tool-name">${tool.name}</div>
                        <div class="detail-tool-input">${JSON.stringify(tool.input, null, 2)}</div>
                    </div>
                `).join('')}
            </div>
        ` : ''}
    `;
  
  // Update navigation buttons and position
  updateModalNavigation();
}

// Update modal navigation buttons and position indicator
function updateModalNavigation() {
  if (!window.messageNavigation) return;
  
  const prevBtn = document.getElementById('modal-prev-btn');
  const nextBtn = document.getElementById('modal-next-btn');
  const position = document.getElementById('modal-position');
  
  // Update position text
  if (position) {
    position.textContent = `${window.messageNavigation.currentIndex + 1} of ${window.messageNavigation.totalMessages}`;
  }
  
  // Update button states
  if (prevBtn) {
    prevBtn.disabled = window.messageNavigation.currentIndex === 0;
  }
  if (nextBtn) {
    nextBtn.disabled = window.messageNavigation.currentIndex === window.messageNavigation.totalMessages - 1;
  }
}

// Navigation helper for "Go to row" functionality
async function goToMessageRow() {
  const input = document.getElementById('messages-row-input');
  if (!input) {return;}
    
  const rowNumber = parseInt(input.value);
  if (!rowNumber || rowNumber < 1) {return;}
    
  // Check if we need to use virtual data
  const isMessagesTab = document.getElementById('messages-tab').classList.contains('active');
  const hasActiveFilters = document.getElementById('type-filter').value ||
                           document.getElementById('error-filter').value ||
                           document.getElementById('tool-filter').value ||
                           document.getElementById('search-input').value;
    
  let totalItems = filteredMessages.length;
  if (isMessagesTab && window.totalMessageCount && !window.messagesFullyLoaded && !hasActiveFilters) {
    totalItems = window.totalMessageCount;
  }
    
  if (rowNumber > totalItems) {
    input.value = '';
    return;
  }
    
  // Calculate which page this row is on
  const targetPage = Math.ceil(rowNumber / messagesPerPage);
  await goToPage(targetPage);
    
  // Highlight the row
  const rowIndex = (rowNumber - 1) % messagesPerPage;
  const tbody = document.querySelector('#messages-tbody');
  if (tbody && tbody.rows[rowIndex]) {
    tbody.rows[rowIndex].scrollIntoView({ behavior: 'smooth', block: 'center' });
    tbody.rows[rowIndex].style.backgroundColor = '#ffffcc';
    setTimeout(() => {
      tbody.rows[rowIndex].style.backgroundColor = '';
    }, 2000);
  }
    
  input.value = '';
}

// Export messages to CSV
async function exportMessagesToCSV() {
  // Ensure all messages are loaded before exporting
  if (!window.messagesFullyLoaded && !window.loadingMoreMessages) {
    console.log('📥 Loading all messages for export...');
    await loadMoreMessages();
  }
  const headers = ['Type', 'Message', 'Timestamp', 'Model', 'Tokens (Input/Output)', 'Tools'];
    
  // Prepare data for export
  const data = filteredMessages.map(msg => {
    const tools = msg.tools.map(t => t.name).join('; ');
    const tokens = `${msg.tokens.input}/${msg.tokens.output}`;
    let msgType = msg.type;
    if (msg.type === 'task' && msg.has_tool_result) {
      msgType = 'task (tool result)';
    } else if (msg.has_tool_result) {
      msgType = 'tool result';
    } else if (msg.type === 'compact_summary') {
      msgType = 'compact summary';
    }
        
    return [
      msgType,
      msg.content || '(empty)',
      formatTimestamp(msg.timestamp),
      msg.model || '-',
      tokens,
      tools || '-'
    ];
  });
    
  const csvContent = exportToCSV(data, headers);
  const timestamp = new Date().toISOString().split('T')[0];
  downloadCSV(csvContent, `messages_export_${timestamp}.csv`);
}

// Export public functions to make them accessible globally
window.initializeFilters = initializeFilters;
window.applyFilters = applyFilters;
window.sortTable = sortTable;
window.displayMessages = displayMessages;
window.changeMessagesPage = changeMessagesPage;
window.goToMessagesPage = goToMessagesPage;
window.updateMessagesPerPage = updateMessagesPerPage;
window.showMessageDetail = showMessageDetail;
window.navigateMessage = navigateMessage;
window.goToMessageRow = goToMessageRow;
window.exportMessagesToCSV = exportMessagesToCSV;