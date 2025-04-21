/**
 * @typedef {import('../../frontend/node_modules/botasaurus-controls/dist/index').Controls} Controls
 */


/**
 * @param {Controls} controls
 */
function getInput(controls) {
    controls
        .listOfTexts('queries', {
            defaultValue: ["Web Developers in Bangalore"],
            placeholder: "Web Developers in Bangalore",
            label: 'Search Queries',
            isRequired: true
        })
        .section("Api Section", (section) => {
            section.text('api_key', {
                placeholder: "2e5d346ap4db8mce4fj7fc112s9h26s61e1192b6a526af51n9",
                label: 'Email and Social Links Extraction API Key',
                helpText: 'Enter your API key to extract email addresses and social media links.',
            })
        })
        .section("Geo Location/ Coordinates", (section) => {
            section
                .text('coordinates', {
                    placeholder: '12.900490, 77.571466'
                })
        })
}