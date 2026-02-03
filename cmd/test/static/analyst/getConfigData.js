// getConfigData.js

export function getConfigData(sections = null) {
    var result=null;

    $.ajax({
        type: "GET",
        url: `/get_config_data`,
        contentType: 'application/json',
        async: false, // This makes the request synchronous
        success: function(response) {
            if (!sections) {
                result = response;
            }
            else if (sections.length == 1) {
                result=[];
                for (let key in response) {
                    if (key == sections) {
                        for (const property in response[key]) {
                            result[property] = (response[key])[property];
                        }
                    }
                }
            }
            else {
                result=[];
                for (let key in response) {
                    for (let section in sections) {
                        if (key == sections[section]) {
                            result[sections[section]] = response[key];
                        }
                    }
                }
            }
        },
        error: function(jqXHR, textStatus, errorThrown) {
            if (jqXHR.responseJSON && jqXHR.responseJSON.detail) {
                // Display specific error from the server
                alert("Error: " + jqXHR.responseJSON.detail);
            } else {
                // Display a generic error message
                alert("Error: " + errorThrown);
            }
        }
    });

    return result;
}
