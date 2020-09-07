package com.home.app.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.home.app.model.Person;
import com.home.app.processor.PersonItemProcessor;



@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;
    
    @Value(value = "input/persons_*.csv")
    private Resource[] resources;

    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName","email","age" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }
    
    @Bean
    public MultiResourceItemReader<Person> multiResourceItemReader(){
    	MultiResourceItemReader<Person> multiResourceItemReader=new MultiResourceItemReader<>();
    	multiResourceItemReader.setResources(resources);
    	multiResourceItemReader.setDelegate(reader());
		return multiResourceItemReader;    	
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }
    
    @Bean
	public FlatFileItemWriter<Person> writer() {
		FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriter<>();
		itemWriter.setResource(new ClassPathResource("output/persons.csv"));
		itemWriter.setAppendAllowed(true);

		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] { "firstName", "lastName", "email", "age" });
		lineAggregator.setFieldExtractor(fieldExtractor);
		itemWriter.setLineAggregator(lineAggregator);
		return itemWriter;
	}

    @Bean
    public Job myJob() {
        return jobBuilderFactory.get("myJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(10)
                .reader(multiResourceItemReader())
                .processor(processor())
                .writer(writer())
                .build();
    }
}