CSS = """
:root {
    --component-height: 30px;
    --bs-body-font-size: 12px;
    --bs-body-font-size-small: 8px;
}

.main {
    min-width: 450px
}

.DateInput_input {
    padding: 4px;
    font-size: var(--bs-body-font-size);
    font-weight: var(--bs-body-font-weight);
    color: var(--bs-gray-dark);
    height: var(--component-height);
}


.DateRangePickerInput .DateInput_input {
    border: 0px;
}

.DateRangePickerInput_clearDates_default:hover {
    background: transparent;
}


.DateInput {
    width: 100px;
    font-size: var(--bs-body-font-size);
    font-weight: var(--bs-body-font-weight);
    color: var(--bs-gray-dark);
    line-height: var(--component-height);
    height: var(--component-height);
}

.DateRangePickerInput_clearDates_svg {
    height: 10px;
}

.DateRangePickerInput__showClearDates {
    padding: 0px;
}

.DateRangePickerInput_clearDates{
    margin: 0px;
}

.DateRangePickerInput_arrow_svg {
    height: 15px;
}

.complex_segment .card-body {
    padding: 0px;
    border-radius: 5px;
}

.Select {
    line-height: var(--component-height);
    min-height: var(--component-height);
    border-radius: 0px;
    font: var(--bs-font-sans-serif);
    font-size: var(--bs-body-font-size);
    font-weight: var(--bs-body-font-weight);
    color: var(--bs-gray-dark);
}

.Select-control {
    line-height: var(--component-height);
    height: var(--component-height);
    border-radius: 0px;
    font: var(--bs-font-sans-serif);
    font-size: var(--bs-body-font-size);
    font-weight: var(--bs-body-font-weight);
    color: var(--bs-gray-dark);
}


.Select--single .Select-multi-value-wrapper{
    line-height: var(--component-height);
    height: var(--component-height);    
}

.Select-value-label {
    color: var(--bs-gray-dark);
    line-height: var(--component-height);
    height: var(--component-height);
    font-size: var(--bs-body-font-size);
}

.Select--multi .Select-value {
    color: var(--bs-gray-dark);
    line-height: calc(var(--component-height));    
    word-wrap: break-word;
    margin-top: 0px;   
}

.Select--multi .Select-value-label {
    padding: 0px 6px;
    line-height: calc(var(--component-height) );    
}

.Select--multi .Select-value-icon {
    padding: 0px 6px;
    line-height: calc(var(--component-height) - 2px);
    height: calc(var(--component-height) - 2px);
    margin-top: 0px;
}



.complex_segment .Select-arrow-zone {
    display: None;
}


.complex_segment .Select-control {
    font-size: var(--bs-body-font-size);
    font-weight: bold;
    color: var(--bs-gray-dark);
    border: 0px;
    background-color: transparent;
}

.complex_segment .Select-menu-outer {
    min-width: 400px;
    white-space: nowrap;
    font-size: var(--bs-body-font-size);
    color: var(--bs-gray-dark);
}

.complex_segment .Select-placeholder {
    font-size: var(--bs-body-font-size);
    font-weight: normal;
    color: var(--bs-gray-dark);
    background-color: transparent;
}

.complex_segment {
    margin-bottom: 5px;
}


.complex_segment .event_name_dropdown.has-value {
    background: rgba(39, 128, 227, 0.2);
}

.complex_segment .simple_segment {
    width: 100%;
    display: flex;
    padding-left: 10px;
}

.complex_segment .simple_segment_with_value {
    width: 100%;
    display: flex;
    padding-left: 10px;
    background: rgba(150, 150, 256, 0.2);
}

.complex_segment .complex_segment_group_by.has-value {
    background: rgba(63, 182, 24, 0.1);
}

.complex_segment_footer {
    border-top: 1px var(--bs-gray-300) solid;
}

.property_operator_dropdown {
    min-width: 70px;
}

.property_name_dropdown {
    min-width: 150px;
}

.metric-type-dropdown {
    width: 180px;
    margin-right: 8px;
}

.metric-type-dropdown-option {
    display: flex;
    align-items: center;
    justify-content: center;
}

.graph_container {
    min-height: 400px;
    text-align: center;
    vertical-align: middle;
    line-height: 400px;
    user-select: none;
}

.graph_auto_refresh .form-label {
    margin-bottom: 0px;
}

.property_name_prefix {
    line-height: var(--bs-body-font-size);
    font-size: var(--bs-body-font-size-small);
    height: var(--bs-body-font-size-small);
    font-weight: normal !important;
}

.Select-value-label .property_name_prefix {
    height: 3px;
}

.property_name {   
    font-size: var(--bs-body-font-size);
}

.input-group .btn {
    z-index: 0 ;
    height: var(--component-height);
    line-height: var(--component-height);
    padding: 0px var(--bs-body-font-size-small);
}

.input-group-text {
    padding: 0px var(--bs-body-font-size-small);
    font-size: var(--bs-body-font-size);
}

.metrics_config_container .input-group {
    margin-bottom: 3px;
}

#navbar_more_dropdown {
    display: none;    
}

#navbar_logo {
    display: none;    
}
"""
