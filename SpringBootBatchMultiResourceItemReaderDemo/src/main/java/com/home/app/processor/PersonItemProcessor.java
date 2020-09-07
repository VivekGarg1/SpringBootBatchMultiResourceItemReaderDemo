package com.home.app.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.home.app.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

	private static final Logger logger = LoggerFactory.getLogger(PersonItemProcessor.class);

	@Override
	public Person process(final Person person) throws Exception {
		final String firstName=person.getFirstName().toUpperCase();
	    final String lastName=person.getLastName().toUpperCase();
	    final Person transformedPerson=new Person(firstName, lastName, person.getEmail(), person.getAge());
	    logger.info("Converting ("+person+") into ("+transformedPerson+")");
		return transformedPerson;
	}

}